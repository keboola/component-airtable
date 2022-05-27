import logging
from typing import Dict, List, Optional

from keboola.component import ComponentBase
from keboola.component.exceptions import UserException
from keboola.utils.header_normalizer import DefaultHeaderNormalizer

from pyairtable import Table

from csv_tools import CachedOrthogonalDictWriter

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


def process_record(record: Dict) -> Dict:
    output_record = {RECORD_ID_FIELD_NAME: record['id'],
                     **record['fields'],
                     RECORD_CREATED_TIME_FIELD_NAME: record['createdTime']}
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
        incremental_loading: bool = params.get(KEY_INCREMENTAL_LOADING, True)

        table = Table(api_key, base_id, table_name)

        args = {}
        if filter_by_formula:
            args['formula'] = filter_by_formula
        if fields:
            args['fields'] = fields

        # Create output table definition
        table_def = self.create_out_table_definition(
            f'{table_name}.csv', incremental=incremental_loading, primary_key=[RECORD_ID_FIELD_NAME])

        # Save the table
        with CachedOrthogonalDictWriter(table_def.full_path, []) as writer:
            for record_batch in table.iterate(**args):
                for record in record_batch:
                    writer.writerow(process_record(record))

        # Save table manifest ({table_name}.csv.manifest) from the table definition
        header_normalizer = DefaultHeaderNormalizer()
        table_def.columns = header_normalizer.normalize_header(
            writer.fieldnames)
        self.write_manifest(table_def)


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
