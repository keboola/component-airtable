import logging
from collections import OrderedDict
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import dateparser

import pyairtable
import pyairtable.metadata
from keboola.component import ComponentBase
from keboola.component.base import sync_action
from keboola.component.dao import (
    TableDefinition,
    SupportedDataTypes,
    ColumnDefinition,
    BaseType,
)
from keboola.component.exceptions import UserException
from keboola.utils.header_normalizer import DefaultHeaderNormalizer
from pyairtable import Api, Base, retry_strategy, Table as ApiTable
from requests import HTTPError

from keboola.csvwriter import ElasticDictWriter
from transformation import ResultTable, RECORD_ID_FIELD_NAME

# Configuration variables
KEY_API_KEY = "#api_key"
KEY_BASE_ID = "base_id"
KEY_TABLE_NAME = "table_name"
KEY_USE_VIEW = "use_view"
KEY_VIEW_NAME = "view_name"
KEY_FIELDS = "fields"
KEY_INCREMENTAL_LOAD = "incremental_loading"
KEY_GROUP_DESTINATION = "destination"

# Sync options variables
KEY_SYNC_OPTIONS = "sync_options"
KEY_SYNC_MODE = "sync_mode"
KEY_SYNC_MODE_INCREMENTAL = "incremental_sync"
KEY_SYNC_DATE_FROM = "date_from"
KEY_SYNC_DATE_TO = "date_to"

# State variables
KEY_STATE_LAST_RUN = "last_run"
KEY_TABLES_COLUMNS = "tables_columns"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_KEY, KEY_BASE_ID, KEY_TABLE_NAME]
REQUIRED_IMAGE_PARS = []

RECORD_CREATED_TIME_FIELD_NAME = "record_created_time"

SUB = "_"
HEADER_NORMALIZER = DefaultHeaderNormalizer(forbidden_sub=SUB)


def normalize_name(name: str):
    return HEADER_NORMALIZER.normalize_header([name])[0]


def process_record(record: Dict) -> Dict:
    fields = record["fields"]
    output_record = {
        RECORD_ID_FIELD_NAME: record["id"],
        RECORD_CREATED_TIME_FIELD_NAME: record["createdTime"],
        **fields,
    }
    return output_record


class Component(ComponentBase):
    """
    Extends base class for general Python components. Initializes the CommonInterface
    and performs configuration validation.

    For easier debugging the data folder is picked up by default from `../data` path,
    relative to working directory.

    If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

        self.table_definitions: Dict[str, TableDefinition] = {}
        self.csv_writers: Dict[str, ElasticDictWriter] = {}
        self.tables_columns = dict()
        self.incremental_destination: bool = False
        self.last_run = int()
        self.state = dict()

    def run(self):
        """
        Main execution code
        """
        # Check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        self.state = self.get_state_file()
        self.last_run = self.state.get(KEY_STATE_LAST_RUN, {}) or []
        self.state[KEY_STATE_LAST_RUN] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.date_from = self._get_date_from()
        self.date_to = self._get_date_to()
        self.state[KEY_TABLES_COLUMNS] = self.tables_columns = self.state.get(KEY_TABLES_COLUMNS, {})

        params: dict = self.configuration.parameters
        # Access parameters in data/config.json
        api_key: str = params[KEY_API_KEY]
        base_id: str = params[KEY_BASE_ID]
        table_id: str = params[KEY_TABLE_NAME]
        view_id: Optional[str] = params.get(KEY_VIEW_NAME)
        fields: Optional[List[str]] = params.get(KEY_FIELDS, None)
        self.incremental_destination: bool = params.get(KEY_GROUP_DESTINATION, {KEY_INCREMENTAL_LOAD: True}).get(
            KEY_INCREMENTAL_LOAD
        )

        api_options = {}
        if self._fetching_is_incremental():
            api_options["formula"] = self._create_filter()
        if fields:
            api_options["fields"] = fields
        if view_id:
            api_options["view"] = view_id

        retry = retry_strategy(status_forcelist=(429, 500, 502, 503, 504), backoff_factor=0.5, total=10)

        try:
            api_table = pyairtable.Table(api_key, base_id, table_id, retry_strategy=retry)
            destination_table_name = self._get_result_table_name(api_table, table_id)

            logging.info(f"Downloading table: {destination_table_name}")
            record_batch_processed = []
            for record_batch in api_table.iterate(**api_options):
                record_batch_processed.extend([process_record(r) for r in record_batch])
            result_table = ResultTable.from_dicts(
                destination_table_name,
                record_batch_processed,
                id_column_names=[RECORD_ID_FIELD_NAME],
            )

            if result_table:
                self.process_table(result_table, api_table)
            else:
                logging.warning("The result is empty!")

        except HTTPError as err:
            self._handle_http_error(err)

        self.finalize_all_tables()
        self.write_state_file(self.state)

    def _create_keboola_schema(self, api_table: pyairtable.Table):
        """
        Create Keboola schema from Airtable table metadata, filtered by selected fields.
        """
        try:
            table_id = api_table.table_name
            tables = pyairtable.metadata.get_base_schema(api_table)
            table_name = next(table["name"] for table in tables["tables"] if table["id"] == table_id)

            table_schema = pyairtable.metadata.get_table_schema(
                pyairtable.Table(
                    api_key=self.configuration.parameters[KEY_API_KEY],
                    base_id=self.configuration.parameters[KEY_BASE_ID],
                    table_name=table_name,
                )
            )
            schema = OrderedDict()

            # Built-in fields
            schema[normalize_name(RECORD_ID_FIELD_NAME)] = ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.STRING),
                primary_key=True,
                nullable=False,
            )
            schema[normalize_name(RECORD_CREATED_TIME_FIELD_NAME)] = ColumnDefinition(
                data_types=BaseType(dtype=SupportedDataTypes.TIMESTAMP),
                primary_key=False,
            )

            # Get selected fields from configuration (field IDs)
            selected_field_ids = self.configuration.parameters.get(KEY_FIELDS, None)

            # Airtable fields - filter by selected field IDs if provided
            for field in table_schema.get("fields", []):
                field_id = field.get("id", "")

                # If fields are specified, only include those fields
                if selected_field_ids is not None and field_id not in selected_field_ids:
                    continue

                normalized_field_name = normalize_name(field.get("name", ""))
                keboola_type = self._convert_airtable_type(field)

                schema[normalized_field_name] = ColumnDefinition(
                    data_types=BaseType(dtype=keboola_type), primary_key=False
                )

            logging.debug(f"Created schema for table '{table_name}' with {len(schema)} columns")
            return schema
        except Exception as e:
            logging.warning(f"Failed to create schema for table '{table_name}': {e}")
            return OrderedDict()

    def _infer_type_from_value(self, value: Any) -> SupportedDataTypes:
        """Infer Keboola base type from a sample Python value."""
        if isinstance(value, bool):
            return SupportedDataTypes.BOOLEAN
        if isinstance(value, int):
            return SupportedDataTypes.INTEGER
        if isinstance(value, float):
            return SupportedDataTypes.FLOAT
        if isinstance(value, datetime):
            return SupportedDataTypes.TIMESTAMP
        return SupportedDataTypes.STRING

    def _augment_schema_with_table_data(self, table: ResultTable, schema: OrderedDict) -> OrderedDict:
        """Extend schema with columns observed in the flattened table data.

        This method discovers columns that don't exist in the Airtable metadata,
        typically resulting from flattening complex fields (e.g., Assignee -> Assignee_name, Assignee_email).
        For safety, all discovered columns are typed as STRING to avoid type inference issues.
        """
        for row in table.to_dicts():
            for column_name, _ in row.items():
                if column_name in schema:
                    continue
                # Default to STRING for all flattened/derived columns to avoid type inference issues
                schema[column_name] = ColumnDefinition(
                    data_types=BaseType(dtype=SupportedDataTypes.STRING), primary_key=False
                )
        return schema

    def _store_table_columns(self, table_name: str, schema: OrderedDict):
        """Persist column order derived from the schema for later writer initialization."""
        if not schema:
            return
        self.tables_columns[table_name] = list(schema.keys())

    def process_table(self, table: ResultTable, api_table: pyairtable.Table = None):
        table.rename_columns(normalize_name)
        table.name = normalize_name(table.name)

        # Create schema using Airtable metadata
        schema = self._create_keboola_schema(api_table)
        schema = self._augment_schema_with_table_data(table, schema)
        self._store_table_columns(table.name, schema)

        self.table_definitions[table.name] = table_def = self.table_definitions.get(
            table.name,
            self.create_out_table_definition(
                name=f"{table.name}.csv",
                incremental=self.incremental_destination,
                primary_key=table.id_column_names,
                has_header=True,
                schema=schema,  # Pass schema to enable native datatypes
            ),
        )
        self.csv_writers[table.name] = csv_writer = self.csv_writers.get(
            table.name,
            ElasticDictWriter(
                file_path=table_def.full_path,
                fieldnames=self.tables_columns.get(table.name, []),
            ),
        )

        csv_writer.writeheader()
        for row in table.to_dicts():
            try:
                csv_writer.writerow(row)
            except UnicodeEncodeError:
                new_row = self.remove_non_utf8(row)
                csv_writer.writerow(new_row)

    @staticmethod
    def remove_non_utf8(row_dict):
        new_row = {}
        for key, value in row_dict.items():
            if isinstance(value, str):
                original_value = value
                new_value = "".join(char for char in value if char.isprintable())

                if original_value != new_value:
                    logging.info(f"Removed non-printable characters for key '{key}': '{new_value}'")

                new_row[key] = new_value

        return new_row

    @staticmethod
    def _convert_airtable_type(field) -> SupportedDataTypes:
        """Convert Airtable field type to Keboola SupportedDataTypes."""
        field_options = field.get("options", {})
        field_type = field.get("type", "")

        if field_type in ["number", "currency", "percent"]:
            precision = field_options.get("precision", 0)
            if precision == 0:
                return SupportedDataTypes.INTEGER
            else:
                return SupportedDataTypes.FLOAT
        elif field_type in ["autoNumber", "count", "rating"]:
            return SupportedDataTypes.INTEGER
        elif field_type == "checkbox":
            return SupportedDataTypes.BOOLEAN
        elif field_type == "date":
            return SupportedDataTypes.DATE
        elif field_type in ["dateTime", "createdTime", "lastModifiedTime"]:
            return SupportedDataTypes.TIMESTAMP
        else:
            return SupportedDataTypes.STRING

    def finalize_all_tables(self):
        for table_name in self.csv_writers:
            csv_writer = self.csv_writers[table_name]
            table_def = self.table_definitions[table_name]
            self.tables_columns[table_name] = csv_writer.fieldnames
            self.write_manifest(table_def)
            csv_writer.close()

    def _fetching_is_incremental(self) -> bool:
        params = self.configuration.parameters
        loading_options = params.get(KEY_SYNC_OPTIONS, {})
        load_type = loading_options.get(KEY_SYNC_MODE)
        return load_type == "incremental_sync"

    def _get_date_from(self) -> Optional[str]:
        params = self.configuration.parameters
        loading_options = params.get(KEY_SYNC_OPTIONS, {})
        incremental = self._fetching_is_incremental()
        return self._get_parsed_date(loading_options.get(KEY_SYNC_DATE_FROM)) if incremental else None

    def _get_date_to(self) -> Optional[str]:
        params = self.configuration.parameters
        loading_options = params.get(KEY_SYNC_OPTIONS, {})
        incremental = self._fetching_is_incremental()
        return self._get_parsed_date(loading_options.get(KEY_SYNC_DATE_TO)) if incremental else None

    @staticmethod
    def _handle_http_error(error: HTTPError):
        json_message = error.response.json()["error"]

        if error.response.status_code == 401:
            message = (
                "Request failed. Invalid credentials. Please verify your PAT token and the scopes allowed. "
                f'Detail: {json_message["type"]}, {json_message["message"]}'
            )
        else:
            message = f'Request failed: {json_message["type"]}. Details: {json_message["message"]}'
        raise UserException(message) from error

    def _get_result_table_name(self, api_table: pyairtable.Table, table_name: str) -> str:

        destination_name = self.configuration.parameters.get(KEY_GROUP_DESTINATION, {}).get(KEY_TABLE_NAME, "")

        if not destination_name:
            # see comments in list_fields() why it is necessary to use get_base_schema()
            tables = pyairtable.metadata.get_base_schema(api_table)
            destination_name = next(table["name"] for table in tables["tables"] if table["id"] == table_name)
        return destination_name

    def _get_parsed_date(self, date_input: Optional[str]) -> Optional[str]:
        if not date_input:
            parsed_date = None
        elif date_input.lower() in ["last", "last run"] and self.last_run:
            parsed_date = dateparser.parse(self.last_run)
        elif date_input.lower() in ["now", "today"]:
            parsed_date = datetime.now(timezone.utc)
        elif date_input.lower() in ["last", "last run"] and not self.last_run:
            parsed_date = dateparser.parse("1990-01-01")
        else:
            try:
                parsed_date = dateparser.parse(date_input).date()
            except (AttributeError, TypeError) as err:
                raise UserException(f"Cannot parse date input {date_input}") from err
        if parsed_date:
            parsed_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
        return parsed_date

    def _create_filter(self) -> str:
        date_from = f"SET_TIMEZONE('{self._get_date_from()}','UTC')"
        date_to = f"SET_TIMEZONE('{self._get_date_to()}','UTC')"
        c_time = "SET_TIMEZONE(CREATED_TIME(),'UTC')"
        l_time = "SET_TIMEZONE(LAST_MODIFIED_TIME(),'UTC')"
        if_not = f"IF(NOT(LAST_MODIFIED_TIME()),{c_time},{l_time})"
        after = f"IS_AFTER({if_not},{date_from})"
        before = f"IS_BEFORE({if_not},{date_to})"
        filter = f"AND({after},{before})"
        return filter

    def _get_table_in_base_schema(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException("API key or personal token is missing")
        base_id: str = params.get(KEY_BASE_ID)
        if not base_id:
            raise UserException("Base ID is missing")
        table_name: str = params.get(KEY_TABLE_NAME)
        if not table_name:
            raise UserException("ResultTable name is missing")
        table = ApiTable(api_key, base_id, table_name)
        base_schema = pyairtable.metadata.get_base_schema(table)
        table_record = None
        for record in base_schema.get("tables", []):
            if record["id"] == table_name:
                table_record = record
                break
        return table_record

    def _list_table_attributes(self, key):
        table = self._get_table_in_base_schema()
        if not table:
            return []
        attributes = [dict(value=field["id"], label=f"{field['name']} ({field['id']})") for field in table.get(key, [])]
        return attributes

    @sync_action("list_fields")
    def list_fields(self):
        fields = self._list_table_attributes("fields")
        return fields

    @sync_action("list_views")
    def list_views(self):
        views = self._list_table_attributes("views")
        return views

    @sync_action("list_bases")
    def list_bases(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException("API key or personal token missing")
        api = Api(api_key)
        bases = pyairtable.metadata.get_api_bases(api)
        resp = [dict(value=base["id"], label=f"{base['name']} ({base['id']})") for base in bases["bases"]]
        return resp

    @sync_action("testConnection")
    def test_connection(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException("API key or personal token missing")
        api = Api(api_key)
        try:
            pyairtable.metadata.get_api_bases(api)
        except Exception as e:
            raise UserException("Login failed! Please check your API Token.") from e

    @sync_action("list_tables")
    def list_tables(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException("API key or personal token is missing")
        base_id: str = params.get(KEY_BASE_ID)
        if not base_id:
            raise UserException("Base ID is missing")
        base = Base(api_key, base_id)
        tables = pyairtable.metadata.get_base_schema(base)
        resp = [dict(value=table["id"], label=f"{table['name']} ({table['id']})") for table in tables["tables"]]
        return resp


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
