import logging
import os
from typing import Dict, List, Optional

import pyairtable
import pyairtable.metadata
from keboola.component import ComponentBase
from keboola.component.base import sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.utils.header_normalizer import DefaultHeaderNormalizer
from pyairtable import Api, Base, Table as ApiTable
from requests import HTTPError

from csv_tools import CachedOrthogonalDictWriter
from transformation import ResultTable, KeboolaDeleteWhereSpec

# Configuration variables
KEY_API_KEY = "#api_key"
KEY_BASE_ID = "base_id"
KEY_TABLE_NAME = "table_name"
KEY_FILTER_BY_FORMULA = "filter_by_formula"
KEY_FIELDS = "fields"
KEY_INCREMENTAL_LOADING = "incremental_loading"
KEY_GROUP_DESTINATION = "destination"

# State variables
KEY_TABLES_COLUMNS = "tables_columns"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_KEY, KEY_BASE_ID, KEY_TABLE_NAME]
REQUIRED_IMAGE_PARS = []

RECORD_ID_FIELD_NAME = "record_id"
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
        self.csv_writers: Dict[str, CachedOrthogonalDictWriter] = {}
        self.delete_where_specs: Dict[str, Optional[KeboolaDeleteWhereSpec]] = {}
        self.tables_columns = dict()
        self.incremental_loading: bool = False
        self.state = dict()

    def run(self):
        """
        Main execution code
        """
        # Check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        self.state = self.get_state_file()
        params: dict = self.configuration.parameters
        # Access parameters in data/config.json
        api_key: str = params[KEY_API_KEY]
        base_id: str = params[KEY_BASE_ID]
        table_id: str = params[KEY_TABLE_NAME]
        filter_by_formula: Optional[str] = params.get(KEY_FILTER_BY_FORMULA, None)
        fields: Optional[List[str]] = params.get(KEY_FIELDS, None)
        self.incremental_loading: bool = params.get(KEY_GROUP_DESTINATION, {KEY_INCREMENTAL_LOADING: True}) \
            .get(KEY_INCREMENTAL_LOADING)

        self.state[KEY_TABLES_COLUMNS] = self.tables_columns = self.state.get(
            KEY_TABLES_COLUMNS, {}
        )

        api_table = pyairtable.Table(api_key, base_id, table_id)
        destination_table_name = self._get_result_table_name(api_table, table_id)

        logging.info(f"Downloading table: {destination_table_name}")
        api_options = {}
        if filter_by_formula:
            api_options["formula"] = filter_by_formula
        if fields:
            api_options["fields"] = fields
        try:
            for i, record_batch in enumerate(api_table.iterate(**api_options)):
                record_batch_processed = [process_record(r) for r in record_batch]
                result_table = ResultTable.from_dicts(
                    destination_table_name, record_batch_processed, id_column_name=RECORD_ID_FIELD_NAME
                )

                if result_table:
                    self.process_table(result_table, str(i))
                    self.finalize_all_tables()
                    self.write_state_file(self.state)
                else:
                    logging.warning("The result is empty!")

        except HTTPError as err:
            self._handle_http_error(err)

    @sync_action('list_bases')
    def list_bases(self):
        params: dict = self.configuration.parameters
        api_key: str = params.get(KEY_API_KEY)
        if not api_key:
            raise UserException('API key or personal token missing')
        api = Api(api_key)
        bases = pyairtable.metadata.get_api_bases(api)
        resp = [dict(value=base['id'], label=f"{base['name']} ({base['id']})") for base in bases['bases']]
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
            raise UserException("Login failed! Please check your API Token.") from e

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
        resp = [dict(value=table['id'], label=f"{table['name']} ({table['id']})") for table in tables['tables']]
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
                incremental=self.incremental_loading,
                primary_key=[table.id_column_name],
                is_sliced=True,
            ),
        )
        self.csv_writers[table.name] = csv_writer = self.csv_writers.get(
            table.name,
            CachedOrthogonalDictWriter(
                file_path=f"{table_def.full_path}/{slice_name}.csv",
                fieldnames=self.tables_columns.get(table.name, []),
            ),
        )
        self.delete_where_specs[
            table.name
        ] = delete_where_spec = self.delete_where_specs.get(
            table.name, table.delete_where_spec
        )

        os.makedirs(table_def.full_path, exist_ok=True)
        csv_writer.writerows(table.to_dicts())
        if table.delete_where_spec:
            assert table.delete_where_spec.column == delete_where_spec.column
            assert table.delete_where_spec.operator == delete_where_spec.operator
            delete_where_spec.values.update(table.delete_where_spec.values)

        for child_table in table.child_tables.values():
            self.process_table(child_table, slice_name)

    def finalize_all_tables(self):
        for table_name in self.csv_writers:
            csv_writer = self.csv_writers[table_name]
            table_def = self.table_definitions[table_name]
            self.tables_columns[table_name] = table_def.columns = csv_writer.fieldnames
            delete_where_spec = self.delete_where_specs[table_name]
            if delete_where_spec:
                table_def.set_delete_where_from_dict(
                    {
                        "column": delete_where_spec.column,
                        "operator": delete_where_spec.operator,
                        "values": list(delete_where_spec.values),
                    }
                )
            self.write_manifest(table_def)
            csv_writer.close()

    @staticmethod
    def _handle_http_error(error: HTTPError):
        json_message = error.response.json()["error"]
        message = f'Request failed: {json_message["type"]}. Details: {json_message["message"]}'
        raise UserException(message) from error

    def _get_result_table_name(self, api_table: pyairtable.Table, table_name: str) -> str:

        destination_name = self.configuration.parameters.get(KEY_GROUP_DESTINATION, {KEY_TABLE_NAME: ''}).get(
            KEY_TABLE_NAME)

        if not destination_name:
            # see comments in list_fields() why it is necessary to use get_base_schema()
            tables = pyairtable.metadata.get_base_schema(api_table)
            destination_name = next(table['name'] for table in tables['tables'] if table['id'] == table_name)
        return destination_name


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
