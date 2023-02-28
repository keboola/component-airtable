import logging
import os
from typing import Dict, List, Optional

from keboola.component import ComponentBase
from keboola.component.base import sync_action
from keboola.component.dao import TableDefinition
from keboola.component.exceptions import UserException
from keboola.utils.header_normalizer import DefaultHeaderNormalizer

import pyairtable
import pyairtable.metadata
from pyairtable import Api, Base, Table as ApiTable

from transformation import Table, KeboolaDeleteWhereSpec
from csv_tools import CachedOrthogonalDictWriter

# Configuration variables
KEY_API_KEY = "#api_key"
KEY_BASE_ID = "base_id"
KEY_TABLE_NAME = "table_name"
KEY_FILTER_BY_FORMULA = "filter_by_formula"
KEY_FIELDS = "fields"
KEY_INCREMENTAL_LOADING = "incremental_loading"

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

    def run(self):
        """
        Main execution code
        """
        # Check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params: dict = self.configuration.parameters
        # Access parameters in data/config.json
        api_key: str = params[KEY_API_KEY]
        base_id: str = params[KEY_BASE_ID]
        table_name: str = params[KEY_TABLE_NAME]
        filter_by_formula: Optional[str] = params.get(KEY_FILTER_BY_FORMULA, None)
        fields: Optional[List[str]] = params.get(KEY_FIELDS, None)
        self.incremental_loading: bool = params.get(KEY_INCREMENTAL_LOADING, True)
        self.state = self.get_state_file()
        self.state[KEY_TABLES_COLUMNS] = self.tables_columns = self.state.get(
            KEY_TABLES_COLUMNS, {}
        )
        self.table_definitions: Dict[str, TableDefinition] = {}
        self.csv_writers: Dict[str, CachedOrthogonalDictWriter] = {}
        self.delete_where_specs: Dict[str, Optional[KeboolaDeleteWhereSpec]] = {}

        api_table = pyairtable.Table(api_key, base_id, table_name)

        api_options = {}
        if filter_by_formula:
            api_options["formula"] = filter_by_formula
        if fields:
            api_options["fields"] = fields

        for i, record_batch in enumerate(api_table.iterate(**api_options)):
            record_batch_processed = [process_record(r) for r in record_batch]
            table = Table.from_dicts(
                table_name, record_batch_processed, id_column_name=RECORD_ID_FIELD_NAME
            )
            self.process_table(table, str(i))

        self.finalize_all_tables()
        self.write_state_file(self.state)

    @sync_action('list_bases')
    def list_bases(self):
        params: dict = self.configuration.parameters
        api_key: str = params[KEY_API_KEY]
        api = Api(api_key)
        bases = pyairtable.metadata.get_api_bases(api)
        resp = [dict(value=base['id'], label=f"{base['name']} ({base['id']})") for base in bases['bases']]
        return resp

    @sync_action('list_tables')
    def list_tables(self):
        params: dict = self.configuration.parameters
        api_key: str = params[KEY_API_KEY]
        base_id: str = params[KEY_BASE_ID]
        base = Base(api_key, base_id)
        tables = pyairtable.metadata.get_base_schema(base)
        resp = [dict(value=table['id'], label=f"{table['name']} ({table['id']})") for table in tables['tables']]
        return resp

    @sync_action('list_fields')
    def list_fields(self):
        params: dict = self.configuration.parameters
        api_key: str = params[KEY_API_KEY]
        base_id: str = params[KEY_BASE_ID]
        table_name: str = params[KEY_TABLE_NAME]
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

    def process_table(self, table: Table, slice_name: str):
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
