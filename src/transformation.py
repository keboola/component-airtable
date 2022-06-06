from dataclasses import dataclass, field
from enum import Enum
from types import NoneType
from typing import Dict, Optional, Union, Type, List
import json
from functools import reduce
import hashlib

import pandas as pd
import typeguard


def is_type(val, type: Type) -> bool:
    try:
        typeguard.check_type('val', val, type)
    except TypeError:
        return False
    else:
        return True


SEP = '_'
COMPUTED_ID_COLUMN_NAME = 'computed_id'
PARENT_ID_COLUMN_NAME = 'parent_id'


ELEMENTARY_TYPE = Union[int, float, str, bool, NoneType]


class ColumnType(Enum):
    ELEMENTARY = ELEMENTARY_TYPE
    OBJECT = Dict
    ARRAY_OF_ELEMENTARY = List[ELEMENTARY_TYPE]
    ARRAY_OF_OBJECTS = List[Dict]

    @classmethod
    def from_example_value(cls, example_value):
        for t in cls:
            if is_type(example_value, t.value):
                return t
        raise ValueError(f'Unexpected field data type. Got value of "{example_value}"'
                         f' as type "{type(example_value)}".')


@dataclass(slots=True, frozen=True)
class Column:
    name: str
    column_type: ColumnType


@dataclass(slots=True)
class Table:
    name: str
    columns: List[Column]
    df: pd.DataFrame
    id_column: Column
    child_tables: Dict[str, 'Table'] = field(default_factory=dict)
    raw_df: Optional[pd.DataFrame] = None
    # TODO: add delete_where spec - will need to discriminate computed id case and proper id case

    @classmethod
    def from_dicts(cls, name: str, dicts: List[Dict], id_column_name: Optional[str] = None):
        if len(dicts) < 1:
            raise ValueError(
                f'Requested table is empty, Cannot handle empty tables.'
                f' Please, make sure that table "{name}" contains at least one record.')
        raw_df = pd.DataFrame.from_records(dicts)
        df = pd.json_normalize(dicts, sep=SEP)
        df.fillna('', inplace=True)
        first_dict = df.iloc[0].to_dict()
        columns = [Column(name=name, column_type=ColumnType.from_example_value(value))
                   for name, value in first_dict.items()]

        if id_column_name:
            id_column = [c for c in columns if c.name == id_column_name][0]
        else:
            df[COMPUTED_ID_COLUMN_NAME] = [hashlib.md5(
                json.dumps(row_dict, sort_keys=True).encode('utf-8')).hexdigest()
                for row_dict in dicts]
            id_column = Column(name=COMPUTED_ID_COLUMN_NAME,
                               column_type=ColumnType.ELEMENTARY)

        table = cls(name=name, columns=columns.copy(), df=df,
                    raw_df=raw_df, id_column=id_column)
        for column in columns:
            table._process_column(column)
        return table

    def _process_column(self, column: Column):
        if column.column_type is ColumnType.ELEMENTARY:
            return  # no need to do anything
        elif column.column_type is ColumnType.OBJECT:
            return  # no need to do anything (thanks to pd.json_normalize)
        elif column.column_type is ColumnType.ARRAY_OF_ELEMENTARY:
            self.df[column.name] = self.df[column.name].apply(
                lambda v: json.dumps(v) if v else '')
        elif column.column_type is ColumnType.ARRAY_OF_OBJECTS:
            child_table_name = f'{self.name}__{column.name}'
            child_table_parts: List[Table] = []
            for i, row_value in self.df[column.name].iteritems():
                if is_type(row_value, column.column_type.value):
                    for e in row_value:
                        e[PARENT_ID_COLUMN_NAME] = self.df[self.id_column.name][i]
                    child_table_parts.append(
                        self.__class__.from_dicts(name=child_table_name, dicts=row_value))
            if child_table_parts:
                self.child_tables[child_table_name] = reduce(
                    lambda x, y: x + y, child_table_parts)
            self.columns.remove(column)
            self.df.drop([column.name], axis='columns', inplace=True)
        else:
            raise ValueError(
                f'Invalid column data type: {column.column_type}.')

    def __add__(self, other: 'Table'):
        assert self.name == other.name
        assert self.columns == other.columns
        assert self.id_column == other.id_column
        df = pd.concat((self.df, other.df), ignore_index=True)
        child_tables: Dict[str, 'Table'] = {}
        for others_child_table_name, others_child_table in other.child_tables.items():
            if others_child_table_name in self.child_tables:
                child_tables[others_child_table_name] = \
                    self.child_tables[others_child_table_name] + \
                    others_child_table
            else:
                child_tables[others_child_table_name] = others_child_table
        return self.__class__(name=self.name, columns=self.columns, df=df,
                              child_tables=child_tables, id_column=self.id_column)
