import logging
import os
from typing import Dict, List, Optional
from datetime import datetime
import dateparser

import pyairtable
import pyairtable.metadata
from keboola.component import ComponentBase
from keboola.component.base import sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.utils.header_normalizer import DefaultHeaderNormalizer
from pyairtable import Api, Base, Table as ApiTable
from requests import HTTPError

from keboola.csvwriter import ElasticDictWriter
from transformation import ResultTable, RECORD_ID_FIELD_NAME

# Configuration variables
KEY_API_KEY = "#api_key"
KEY_BASE_ID = "base_id"
KEY_TABLE_NAME = "table_name"
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
REQUIRED_PARAMETERS = [KEY_API_KEY, KEY_BASE_ID,
                       KEY_TABLE_NAME]
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
        self.state[KEY_STATE_LAST_RUN] = datetime.now().strftime(
            "%Y-%m-%d %H:%M:%S")
        self.date_from = self._get_date_from()
        self.date_to = self._get_date_to()
        self.state[KEY_TABLES_COLUMNS] = self.tables_columns = self.state.get(
            KEY_TABLES_COLUMNS, {}
        )

        params: dict = self.configuration.parameters
        # Access parameters in data/config.json
        api_key: str = params[KEY_API_KEY]
        base_id: str = params[KEY_BASE_ID]
        table_id: str = params[KEY_TABLE_NAME]
        fields: Optional[List[str]] = params.get(KEY_FIELDS, None)
        self.incremental_destination: bool = params.get(KEY_GROUP_DESTINATION, {KEY_INCREMENTAL_LOAD: True}) \
            .get(KEY_INCREMENTAL_LOAD)

        api_options = {}
        if self._fetching_is_incremental():
            api_options["formula"] = self._create_filter()
        if fields:
            api_options["fields"] = fields
        try:
            api_table = pyairtable.Table(api_key, base_id, table_id)
            destination_table_name = self._get_result_table_name(
                api_table, table_id)

            logging.info(f"Downloading table: {destination_table_name}")
            for i, record_batch in enumerate(api_table.iterate(**api_options)):
                record_batch_processed = [
                    process_record(r) for r in record_batch]
                result_table = ResultTable.from_dicts(
                    destination_table_name, record_batch_processed, id_column_names=[
                        RECORD_ID_FIELD_NAME]
                )

                if result_table:
                    self.process_table(result_table, str(i))
                else:
                    logging.warning("The result is empty!")

        except HTTPError as err:
            self._handle_http_error(err)

        self.finalize_all_tables()
        self.write_state_file(self.state)

    @sync_action('list_bases')
    def list_bases(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException('API key or personal token missing')
        api = Api(api_key)
        bases = pyairtable.metadata.get_api_bases(api)
        resp = [dict(
            value=base['id'], label=f"{base['name']} ({base['id']})") for base in bases['bases']]
        return resp

    @sync_action('testConnection')
    def test_connection(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException('API key or personal token missing')
        api = Api(api_key)
        try:
            pyairtable.metadata.get_api_bases(api)
        except Exception as e:
            raise UserException(
                "Login failed! Please check your API Token.") from e

    @sync_action('list_tables')
    def list_tables(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException('API key or personal token is missing')
        base_id: str = params.get(KEY_BASE_ID)
        if not base_id:
            raise UserException('Base ID is missing')
        base = Base(api_key, base_id)
        tables = pyairtable.metadata.get_base_schema(base)
        resp = [dict(
            value=table['id'], label=f"{table['name']} ({table['id']})") for table in tables['tables']]
        return resp

    @sync_action('list_fields')
    def list_fields(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException('API key or personal token is missing')
        base_id: str = params.get(KEY_BASE_ID)
        if not base_id:
            raise UserException('Base ID is missing')
        table_name: str = params.get(KEY_TABLE_NAME)
        if not table_name:
            raise UserException('ResultTable name is missing')
        table = ApiTable(api_key, base_id, table_name)
        # we cannot use library method get_table_schema se it searches by table name
        #    fields = pyairtable.metadata.get_table_schema(table)
        # we must use our own version searching for a table id
        base_schema = pyairtable.metadata.get_base_schema(table)
        table_record = None
        for record in base_schema.get("tables", []):
            if record["id"] == table_name:
                table_record = record
                break
        if not table_record:
            return []
        resp = [dict(value=field['id'], label=f"{field['name']} ({field['id']})") for field in
                table_record.get('fields', [])]
        return resp

    def process_table(self, table: ResultTable, slice_name: str):
        table.rename_columns(normalize_name)
        table.name = normalize_name(table.name)

        self.table_definitions[table.name] = table_def = self.table_definitions.get(
            table.name,
            self.create_out_table_definition(
                name=f"{table.name}.csv",
                incremental=self.incremental_destination,
                primary_key=table.id_column_names,
                is_sliced=True,
            ),
        )
        self.csv_writers[table.name] = csv_writer = self.csv_writers.get(
            table.name,
            ElasticDictWriter(
                file_path=f"{table_def.full_path}/{slice_name}.csv",
                fieldnames=self.tables_columns.get(table.name, []),
            ),
        )
        os.makedirs(table_def.full_path, exist_ok=True)
        csv_writer.writerows(table.to_dicts())

        for child_table in table.child_tables.values():
            self.process_table(child_table, slice_name)

    def finalize_all_tables(self):
        for table_name in self.csv_writers:
            csv_writer = self.csv_writers[table_name]
            table_def = self.table_definitions[table_name]
            self.tables_columns[table_name] = table_def.columns = csv_writer.fieldnames
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
            message = 'Request failed. Invalid credentials. Please verify your PAT token and the scopes allowed. ' \
                      f'Detail: {json_message["type"]}, {json_message["message"]}'
        else:
            message = f'Request failed: {json_message["type"]}. Details: {json_message["message"]}'
        raise UserException(message) from error

    def _get_result_table_name(self, api_table: pyairtable.Table, table_name: str) -> str:

        destination_name = self.configuration.parameters.get(KEY_GROUP_DESTINATION, {KEY_TABLE_NAME: ''}).get(
            KEY_TABLE_NAME)

        if not destination_name:
            # see comments in list_fields() why it is necessary to use get_base_schema()
            tables = pyairtable.metadata.get_base_schema(api_table)
            destination_name = next(
                table['name'] for table in tables['tables'] if table['id'] == table_name)
        return destination_name

    def _get_parsed_date(self, date_input: Optional[str]) -> Optional[str]:
        if not date_input:
            parsed_date = None
        elif date_input.lower() in ["last", "last run"] and self.last_run:
            parsed_date = dateparser.parse(self.last_run)
        elif date_input.lower() in ["now", "today"]:
            parsed_date = datetime.now()
        elif date_input.lower() in ["last", "last run"] and not self.last_run:
            parsed_date = dateparser.parse("1990-01-01")
        else:
            try:
                parsed_date = dateparser.parse(date_input).date()
            except (AttributeError, TypeError) as err:
                raise UserException(
                    f"Cannot parse date input {date_input}") from err
        if parsed_date:
            parsed_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S")
        return parsed_date

    def _create_filter(self) -> str:
        filter = (f"AND(IS_AFTER(IF(NOT(LAST_MODIFIED_TIME()),CREATED_TIME(),LAST_MODIFIED_TIME()),"
                  f"'{self._get_date_from()}'),"
                  f"IS_BEFORE(IF(NOT(LAST_MODIFIED_TIME()),CREATED_TIME(),LAST_MODIFIED_TIME()),"
                  f"'{self._get_date_to()}'))"
                  )
        return filter


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
