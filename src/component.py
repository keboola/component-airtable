import logging
import os
from typing import Dict, List, Optional

from keboola.component import ComponentBase
from keboola.component.exceptions import UserException
from keboola.utils.header_normalizer import DefaultHeaderNormalizer

import pyairtable

from transformation import Table

# configuration variables
KEY_API_KEY = "#api_key"
KEY_BASE_ID = "base_id"
KEY_TABLE_NAME = "table_name"
KEY_FILTER_BY_FORMULA = "filter_by_formula"
KEY_FIELDS = "fields"
KEY_INCREMENTAL_LOADING = "incremental_loading"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_API_KEY, KEY_BASE_ID, KEY_TABLE_NAME]
REQUIRED_IMAGE_PARS = []

RECORD_ID_FIELD_NAME = 'record_id'
RECORD_CREATED_TIME_FIELD_NAME = 'record_created_time'

SUB = '_'
HEADER_NORMALIZER = DefaultHeaderNormalizer(forbidden_sub=SUB)


def process_record(record: Dict) -> Dict:
    fields = record['fields']
    output_record = {RECORD_ID_FIELD_NAME: record['id'],
                     RECORD_CREATED_TIME_FIELD_NAME: record['createdTime'],
                     **fields}
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
        # Check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)

    def run(self):
        '''
        Main execution code
        '''
        params: dict = self.configuration.parameters
        # Access parameters in data/config.json
        api_key: str = params[KEY_API_KEY]
        base_id: str = params[KEY_BASE_ID]
        table_name: str = params[KEY_TABLE_NAME]
        filter_by_formula: Optional[str] = params.get(
            KEY_FILTER_BY_FORMULA, None)
        fields: Optional[List[str]] = params.get(KEY_FIELDS, None)
        self.incremental_loading: bool = params.get(
            KEY_INCREMENTAL_LOADING, True)

        api_table = pyairtable.Table(api_key, base_id, table_name)

        api_options = {}
        if filter_by_formula:
            api_options['formula'] = filter_by_formula
        if fields:
            api_options['fields'] = fields

        for i, record_batch in enumerate(api_table.iterate(**api_options)):
            record_batch_processed = [process_record(r) for r in record_batch]
            table = Table.from_dicts(table_name, record_batch_processed,
                                     id_column_name=RECORD_ID_FIELD_NAME)
            self.save_table(table, str(i))

    def save_table(self, table: Table, slice_name: str):
        table_def = self.create_out_table_definition(
            f'{HEADER_NORMALIZER.normalize_header([table.name])[0]}.csv',
            incremental=self.incremental_loading,
            primary_key=[table.id_column.name],
            is_sliced=True)
        os.makedirs(table_def.full_path, exist_ok=True)
        table.df.to_csv(f'{table_def.full_path}/{slice_name}.csv',
                        index=False, header=False)
        table_def.columns = HEADER_NORMALIZER.normalize_header(
            col.name for col in table.columns)
        self.write_manifest(table_def)
        for child_table in table.child_tables.values():
            self.save_table(child_table, slice_name)


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
