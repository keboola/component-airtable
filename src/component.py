import logging
from collections import OrderedDict
from dataclasses import dataclass, field
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
    incremental_loading: bool = True
    sync_options: dict[str, Any] = field(default_factory=dict)
    destination: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_parameters(cls, params: dict[str, Any]) -> "AirtableConfig":
        """Create configuration from raw parameters dictionary."""
        destination = params.get(KEY_GROUP_DESTINATION, {})
        return cls(
            api_key=params[KEY_API_KEY],
            base_id=params[KEY_BASE_ID],
            table_name=params[KEY_TABLE_NAME],
            view_name=params.get(KEY_VIEW_NAME),
            fields=params.get(KEY_FIELDS),
            incremental_loading=destination.get(KEY_INCREMENTAL_LOAD, True),
            sync_options=params.get(KEY_SYNC_OPTIONS, {}),
            destination=destination,
        )


def normalize_name(name: str) -> str:
    return HEADER_NORMALIZER.normalize_header([name])[0]


def process_record(record: dict) -> dict:
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

        # Validate and load configuration
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        self.config = AirtableConfig.from_parameters(self.configuration.parameters)

        # Initialize API client with retry strategy
        retry = retry_strategy(status_forcelist=(429, 500, 502, 503, 504), backoff_factor=0.5, total=10)
        self.api_table = ApiTable(
            self.config.api_key, self.config.base_id, self.config.table_name, retry_strategy=retry
        )
        self.api = Api(self.config.api_key)

        # Initialize state
        self.state: dict = self.get_state_file()
        self.last_run: str | None = self.state.get(KEY_STATE_LAST_RUN) or None
        self.date_from: str | None = None
        self.date_to: str | None = None

        # Initialize data structures
        self.table_definitions: dict[str, TableDefinition] = {}
        self.csv_writers: dict[str, ElasticDictWriter] = {}
        self.tables_columns: dict[str, list[str]] = self.state.get(KEY_TABLES_COLUMNS, {})
        self.incremental_destination: bool = self.config.incremental_loading

    def run(self) -> None:
        """
        Main execution code
        """
        # Update state with current run time
        self.state[KEY_STATE_LAST_RUN] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.date_from = self._get_date_from()
        self.date_to = self._get_date_to()
        self.state[KEY_TABLES_COLUMNS] = self.tables_columns

        # Build API options for fetching data
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

    def _fetch_airtable_field_types(self, api_table: pyairtable.Table) -> dict[str, SupportedDataTypes]:
        """
        Fetch Airtable metadata and return a mapping of normalized field names to Keboola types.
        """
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
            for air_field in table_schema.get("fields", []):
                normalized_name = normalize_name(air_field.get("name", ""))
                field_type_map[normalized_name] = self._convert_airtable_type(air_field)
        except (KeyError, StopIteration, ValueError) as e:
            logging.warning(f"Failed to fetch Airtable metadata: {e}. All fields will default to STRING.")

        return field_type_map

    def _create_keboola_schema(
        self, result_table: ResultTable, field_type_map: dict[str, SupportedDataTypes] | None = None
    ) -> OrderedDict[str, ColumnDefinition]:
        """
        Create Keboola schema based on actual ResultTable columns.

        Args:
            result_table: The table with actual data
            field_type_map: Pre-fetched mapping of normalized field names to Keboola types
        """
        schema = OrderedDict()
        field_type_map = field_type_map or {}

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

        # Get columns from first row (all rows should have same structure after processing)
        actual_columns = set()
        if result_table.rows:
            actual_columns = set(result_table.rows[0].keys())

        # Add schema for all actual columns (except built-ins already added)
        for column_name in actual_columns:
            if column_name in schema:
                continue

            # Try to get type from Airtable metadata, otherwise default to STRING
            keboola_type = field_type_map.get(column_name, SupportedDataTypes.STRING)
            schema[column_name] = ColumnDefinition(data_types=BaseType(dtype=keboola_type), primary_key=False)

        logging.debug(f"Created schema with {len(schema)} columns from ResultTable")
        return schema

    def _store_table_columns(self, table_name: str, schema: OrderedDict[str, ColumnDefinition]) -> None:
        """Persist column order derived from the schema for later writer initialization."""
        if not schema:
            return
        self.tables_columns[table_name] = list(schema.keys())

    def initialize_table(self, table: ResultTable, api_table: pyairtable.Table) -> None:
        """Initialize table schema, definition, and CSV writer (called once per table)."""
        table.rename_columns(normalize_name)
        table.name = normalize_name(table.name)

        # Fetch metadata once
        field_type_map = self._fetch_airtable_field_types(api_table)

        # Create schema with fetched metadata
        schema = self._create_keboola_schema(table, field_type_map)
        self._store_table_columns(table.name, schema)

        # Create table definition
        table_def = self.create_out_table_definition(
            name=f"{table.name}.csv",
            incremental=self.incremental_destination,
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

    def process_table(self, table: ResultTable) -> None:
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
    def remove_non_utf8(row_dict: dict) -> dict:
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
    def _convert_airtable_type(field: dict) -> SupportedDataTypes:
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

    def finalize_all_tables(self) -> None:
        for table_name in self.csv_writers:
            csv_writer = self.csv_writers[table_name]
            table_def = self.table_definitions[table_name]
            self.tables_columns[table_name] = csv_writer.fieldnames
            self.write_manifest(table_def)
            csv_writer.writeheader()
            csv_writer.close()

    def _fetching_is_incremental(self) -> bool:
        load_type = self.config.sync_options.get(KEY_SYNC_MODE)
        return load_type == "incremental_sync"

    def _get_date_from(self) -> str | None:
        incremental = self._fetching_is_incremental()
        return self._get_parsed_date(self.config.sync_options.get(KEY_SYNC_DATE_FROM)) if incremental else None

    def _get_date_to(self) -> str | None:
        incremental = self._fetching_is_incremental()
        return self._get_parsed_date(self.config.sync_options.get(KEY_SYNC_DATE_TO)) if incremental else None

    @staticmethod
    def _handle_http_error(error: HTTPError) -> None:
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
        destination_name = self.config.destination.get(KEY_TABLE_NAME, "")

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
        formula = f"AND({after},{before})"
        return formula

    def _get_table_in_base_schema(self) -> dict[str, Any] | None:
        """Get table metadata from base schema using the configured api_table."""
        base_schema = pyairtable.metadata.get_base_schema(self.api_table)
        table_record = None
        for record in base_schema.get("tables", []):
            if record["id"] == self.config.table_name:
                table_record = record
                break
        return table_record

    def _list_table_attributes(self, key: str) -> list[dict[str, str]]:
        """List attributes (fields or views) from table metadata."""
        table = self._get_table_in_base_schema()
        if not table:
            return []
        attributes = [dict(value=field["id"], label=f"{field['name']} ({field['id']})") for field in table.get(key, [])]
        return attributes

    @sync_action("list_fields")
    def list_fields(self) -> list[dict[str, str]]:
        """List available fields from the configured table."""
        return self._list_table_attributes("fields")

    @sync_action("list_views")
    def list_views(self) -> list[dict[str, str]]:
        """List available views from the configured table."""
        return self._list_table_attributes("views")

    @sync_action("list_bases")
    def list_bases(self) -> list[dict[str, str]]:
        """List all bases accessible with the configured API key."""
        bases = pyairtable.metadata.get_api_bases(self.api)
        return [dict(value=base["id"], label=f"{base['name']} ({base['id']})") for base in bases["bases"]]

    @sync_action("testConnection")
    def test_connection(self) -> None:
        """Test the API connection with the configured credentials."""
        try:
            pyairtable.metadata.get_api_bases(self.api)
        except (HTTPError, ValueError) as e:
            raise UserException("Login failed! Please check your API Token.") from e

    @sync_action("list_tables")
    def list_tables(self) -> list[dict[str, str]]:
        """List all tables in the configured base."""
        base = Base(self.config.api_key, self.config.base_id)
        tables = pyairtable.metadata.get_base_schema(base)
        return [dict(value=table["id"], label=f"{table['name']} ({table['id']})") for table in tables["tables"]]


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
