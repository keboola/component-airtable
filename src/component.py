import logging
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import dateparser
import pyairtable
import pyairtable.metadata
from keboola.component import ComponentBase
from keboola.component.base import sync_action
from keboola.component.dao import (
    BaseType,
    ColumnDefinition,
    SupportedDataTypes,
    TableDefinition,
)
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter
from keboola.utils.header_normalizer import DefaultHeaderNormalizer
from pyairtable import Api, Base, retry_strategy
from pyairtable import Table as ApiTable
from requests import HTTPError

from transformation import RECORD_ID_FIELD_NAME, ResultTable

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


@dataclass
class AirtableConfig:
    """Configuration for Airtable component."""

    api_key: str
    base_id: str
    table_name: str
    view_name: str | None = None
    fields: list[str] | None = None
    incremental_destination: bool = True
    sync_mode: str | None = None
    date_from: str | None = None
    date_to: str | None = None
    destination_table_name: str = ""

    @classmethod
    def from_parameters(cls, params: dict[str, Any]) -> "AirtableConfig":
        """Create config from component parameters."""
        sync_options = params.get(KEY_SYNC_OPTIONS, {})
        destination = params.get(KEY_GROUP_DESTINATION, {})

        return cls(
            api_key=params[KEY_API_KEY],
            base_id=params[KEY_BASE_ID],
            table_name=params[KEY_TABLE_NAME],
            view_name=params.get(KEY_VIEW_NAME),
            fields=params.get(KEY_FIELDS),
            incremental_destination=destination.get(KEY_INCREMENTAL_LOAD, True),
            sync_mode=sync_options.get(KEY_SYNC_MODE),
            date_from=sync_options.get(KEY_SYNC_DATE_FROM),
            date_to=sync_options.get(KEY_SYNC_DATE_TO),
            destination_table_name=destination.get(KEY_TABLE_NAME, ""),
        )


def normalize_name(name: str):
    return HEADER_NORMALIZER.normalize_header([name])[0]


def process_record(record: dict) -> dict:
    """
    Process Airtable record into output format.

    Airtable API guarantees these fields are always present in records.
    """
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

        # These will be initialized in run() after validation
        self.config: AirtableConfig | None = None
        self.api_table: ApiTable | None = None
        self.retry_strategy = retry_strategy(status_forcelist=(429, 500, 502, 503, 504), backoff_factor=0.5, total=10)

        # Table processing state
        self.table_definitions: dict[str, TableDefinition] = {}
        self.csv_writers: dict[str, ElasticDictWriter] = {}
        self.tables_columns: dict[str, list[str]] = {}

        # Sync state
        self.last_run: str | None = None
        self.state: dict[str, Any] = {}
        self.date_from: str | None = None
        self.date_to: str | None = None

    def run(self):
        """
        Main execution code
        """
        # Check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

        # Initialize configuration
        params = self.configuration.parameters
        self.config = AirtableConfig.from_parameters(params)

        # Initialize state
        self.state = self.get_state_file()
        self.last_run = self.state.get(KEY_STATE_LAST_RUN)
        self.state[KEY_STATE_LAST_RUN] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.date_from = self._get_date_from()
        self.date_to = self._get_date_to()
        self.tables_columns = self.state.get(KEY_TABLES_COLUMNS, {})
        self.state[KEY_TABLES_COLUMNS] = self.tables_columns

        # Initialize API client
        self.api_table = pyairtable.Table(
            self.config.api_key, self.config.base_id, self.config.table_name, retry_strategy=self.retry_strategy
        )

        # Build API options
        api_options = {}
        if self._fetching_is_incremental():
            api_options["formula"] = self._create_filter()
        if self.config.fields:
            api_options["fields"] = self.config.fields
        if self.config.view_name:
            api_options["view"] = self.config.view_name

        try:
            destination_table_name = self._get_result_table_name(self.api_table, self.config.table_name)

            logging.info(f"Downloading table: {destination_table_name}")

            schema_initialized = False
            for record_batch in self.api_table.iterate(**api_options):
                records = [process_record(r) for r in record_batch]
                result_table = ResultTable.from_dicts(
                    destination_table_name,
                    records,
                    id_column_names=[RECORD_ID_FIELD_NAME],
                )

                if result_table:
                    # Initialize schema and writers only once using the first batch
                    if not schema_initialized:
                        self.initialize_table(result_table, self.api_table)
                        schema_initialized = True

                    self.process_table(result_table)
                else:
                    logging.warning("The result is empty!")

        except HTTPError as err:
            self._handle_http_error(err)

        self.finalize_all_tables()
        self.write_state_file(self.state)

    def _create_keboola_schema(self, api_table: pyairtable.Table, result_table: ResultTable):
        """
        Create Keboola schema based on actual ResultTable columns,
        using Airtable metadata for type information where available.
        """
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

        # Get Airtable metadata for type mapping
        field_type_map = {}
        try:
            table_id = api_table.table_name
            tables = pyairtable.metadata.get_base_schema(api_table)
            table_name = next(table["name"] for table in tables["tables"] if table["id"] == table_id)

            table_schema = pyairtable.metadata.get_table_schema(
                pyairtable.Table(
                    api_key=self.config.api_key,
                    base_id=self.config.base_id,
                    table_name=table_name,
                )
            )

            # Build a map of normalized field names to their Airtable types
            for field in table_schema.get("fields", []):
                normalized_name = normalize_name(field.get("name", ""))
                field_type_map[normalized_name] = self._convert_airtable_type(field)
        except Exception as e:
            logging.warning(f"Failed to fetch Airtable metadata: {e}. All fields will default to STRING.")

        # Get all actual columns from the ResultTable
        actual_columns = set()
        for row in result_table.to_dicts():
            actual_columns.update(row.keys())

        # Add schema for all actual columns (except built-ins already added)
        for column_name in actual_columns:
            if column_name in schema:
                continue

            # Try to get type from Airtable metadata, otherwise default to STRING
            keboola_type = field_type_map.get(column_name, SupportedDataTypes.STRING)
            schema[column_name] = ColumnDefinition(data_types=BaseType(dtype=keboola_type), primary_key=False)

        logging.debug(f"Created schema with {len(schema)} columns from ResultTable")
        return schema

    def _store_table_columns(self, table_name: str, schema: OrderedDict):
        """Persist column order derived from the schema for later writer initialization."""
        if not schema:
            return
        self.tables_columns[table_name] = list(schema.keys())

    def initialize_table(self, table: ResultTable, api_table: pyairtable.Table):
        """Initialize table schema, definition, and CSV writer (called once per table)."""
        table.rename_columns(normalize_name)
        table.name = normalize_name(table.name)

        # Create schema based on actual ResultTable columns, using Airtable metadata for types
        schema = self._create_keboola_schema(api_table, table)
        self._store_table_columns(table.name, schema)

        # Create table definition
        table_def = self.create_out_table_definition(
            name=f"{table.name}.csv",
            incremental=self.config.incremental_destination,
            primary_key=table.id_column_names,
            has_header=True,
            schema=schema,  # Pass schema to enable native datatypes
        )
        self.table_definitions[table.name] = table_def

        # Create CSV writer
        fieldnames = self.tables_columns[table.name]
        csv_writer = ElasticDictWriter(
            file_path=table_def.full_path,
            fieldnames=fieldnames,
        )
        self.csv_writers[table.name] = csv_writer

    def process_table(self, table: ResultTable):
        """Process a batch of table data (write rows to CSV)."""
        table.rename_columns(normalize_name)
        table.name = normalize_name(table.name)

        csv_writer = self.csv_writers[table.name]

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
            csv_writer.writeheader()
            csv_writer.close()

    def _fetching_is_incremental(self) -> bool:
        return self.config.sync_mode == "incremental_sync"

    def _get_date_from(self) -> str | None:
        incremental = self._fetching_is_incremental()
        return self._get_parsed_date(self.config.date_from) if incremental else None

    def _get_date_to(self) -> str | None:
        incremental = self._fetching_is_incremental()
        return self._get_parsed_date(self.config.date_to) if incremental else None

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
        destination_name = self.config.destination_table_name

        if not destination_name:
            # see comments in list_fields() why it is necessary to use get_base_schema()
            tables = pyairtable.metadata.get_base_schema(api_table)
            destination_name = next(table["name"] for table in tables["tables"] if table["id"] == table_name)
        return destination_name

    def _get_parsed_date(self, date_input: str | None) -> str | None:
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
        """
        Get table schema for sync actions.

        Note: This is used by sync actions which run before config initialization,
        so it accesses configuration.parameters directly.
        """
        params = self.configuration.parameters
        api_key = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException("API key or personal token is missing")
        base_id = params.get(KEY_BASE_ID)
        if not base_id:
            raise UserException("Base ID is missing")
        table_name = params.get(KEY_TABLE_NAME)
        if not table_name:
            raise UserException("Table name is missing")
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
        return [{"value": field["id"], "label": f"{field['name']} ({field['id']})"} for field in table.get(key, [])]

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
        """Sync action to list available Airtable bases."""
        params = self.configuration.parameters
        api_key = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException("API key or personal token missing")
        api = Api(api_key)
        bases = pyairtable.metadata.get_api_bases(api)
        return [{"value": base["id"], "label": f"{base['name']} ({base['id']})"} for base in bases["bases"]]

    @sync_action("testConnection")
    def test_connection(self):
        """Sync action to test connection with provided API key."""
        params = self.configuration.parameters
        api_key = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException("API key or personal token missing")
        api = Api(api_key)
        try:
            pyairtable.metadata.get_api_bases(api)
        except Exception as e:
            raise UserException("Login failed! Please check your API Token.") from e

    @sync_action("list_tables")
    def list_tables(self):
        """Sync action to list tables in a base."""
        params = self.configuration.parameters
        api_key = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException("API key or personal token is missing")
        base_id = params.get(KEY_BASE_ID)
        if not base_id:
            raise UserException("Base ID is missing")
        base = Base(api_key, base_id)
        tables = pyairtable.metadata.get_base_schema(base)
        return [{"value": table["id"], "label": f"{table['name']} ({table['id']})"} for table in tables["tables"]]


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
