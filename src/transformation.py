from dataclasses import dataclass, field
from enum import Enum
from types import NoneType
from typing import Any, Callable, Dict, Optional, Union, Type, List, Set, MutableMapping
import json
import hashlib

import typeguard


SUBOBJECT_SEP = "_"
CHILD_TABLE_SEP = "__"
COMPUTED_ID_COLUMN_NAME = "computed_id"
PARENT_ID_COLUMN_NAME = "parent_id"


ELEMENTARY_TYPE = Union[int, float, str, bool, NoneType]


def is_type(val, type: Type) -> bool:
    try:
        typeguard.check_type("val", val, type)
    except TypeError:
        return False
    else:
        return True


def flatten_dict(
    dictionary: Dict,
    parent_key: Optional[str] = None,
    separator: str = SUBOBJECT_SEP,
    flatten_lists: bool = False,
):
    items = []
    for key, value in dictionary.items():
        new_key = str(parent_key) + separator + key if parent_key else key
        if isinstance(value, MutableMapping):
            items.extend(flatten_dict(value, new_key, separator).items())
        elif flatten_lists and isinstance(value, list):
            for k, v in enumerate(value):
                items.extend(flatten_dict({str(k): v}, new_key).items())
        else:
            items.append((new_key, value))
    return dict(items)


@dataclass(slots=True)
class KeboolaDeleteWhereSpec:
    column: str
    values: Set[str] = field(default_factory=lambda: set())
    operator: str = "eq"


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
        raise ValueError(
            f'Unexpected field data type. Got value of "{example_value}"'
            f' as type "{type(example_value)}".'
        )


@dataclass(slots=True)
class Table:
    name: str
    id_column_name: str
    rows: List[Dict[str, Any]] = field(default_factory=list)
    child_tables: Dict[str, "Table"] = field(default_factory=dict)
    delete_where_spec: Optional[KeboolaDeleteWhereSpec] = None

    @classmethod
    def from_dicts(
        cls,
        name: str,
        dicts: List[Dict[str, Any]],
        id_column_name: Optional[str] = None,
    ):
        if len(dicts) < 1:
            raise ValueError(
                f"Requested table is empty, Cannot handle empty tables."
                f' Please, make sure that table "{name}" contains at least one record.'
            )
        if not id_column_name:
            id_column_name = COMPUTED_ID_COLUMN_NAME
        table = cls(name=name, id_column_name=id_column_name)
        for row_dict in dicts:
            table.add_row(row_dict)
        return table

    def add_row(self, row_dict: Dict[str, Any]):
        def add_value_to_row(column_name: str, value, row_dict: Dict[str, Any]):
            if not value:
                return
            column_type = ColumnType.from_example_value(value)
            if column_type is ColumnType.ELEMENTARY:
                row_dict[column_name] = value  # no need to do anything
            elif column_type is ColumnType.OBJECT:
                flattened_dict = flatten_dict(value, parent_key=column_name)
                for flattened_key, flattened_value in flattened_dict.items():
                    add_value_to_row(flattened_key, flattened_value, row_dict)
            elif column_type is ColumnType.ARRAY_OF_ELEMENTARY:
                row_dict[column_name] = json.dumps(
                    value
                )  # TODO?: maybe create child table instead?
            elif column_type is ColumnType.ARRAY_OF_OBJECTS:
                child_table_name = f"{self.name}{CHILD_TABLE_SEP}{column_name}"
                self.child_tables[
                    child_table_name
                ] = child_table = self.child_tables.get(
                    child_table_name,
                    self.__class__(
                        name=child_table_name,
                        id_column_name=COMPUTED_ID_COLUMN_NAME,
                        delete_where_spec=KeboolaDeleteWhereSpec(
                            column=PARENT_ID_COLUMN_NAME
                        ),
                    ),
                )
                for child_dict in value:
                    child_dict: Dict
                    child_dict[PARENT_ID_COLUMN_NAME] = parent_id = row_dict[
                        self.id_column_name
                    ]
                    child_table.add_row(child_dict)
                    child_table.delete_where_spec.values.add(parent_id)
            else:
                raise ValueError(f"Invalid column data type: {column_type}.")

        processed_dict = {}
        id_value = row_dict.get(  # Need to process the ID column first
            self.id_column_name,
            hashlib.md5(
                json.dumps(row_dict, sort_keys=True).encode("utf-8")
            ).hexdigest(),  # If there is no ID column value, we use MD5 hash instead
        )
        add_value_to_row(self.id_column_name, id_value, processed_dict)
        for column_name, value in row_dict.items():
            if column_name != self.id_column_name:
                add_value_to_row(column_name, value, processed_dict)
        self.rows.append(processed_dict)

    def rename_columns(self, rename_function: Callable[[str], str]):
        self.rows = [
            {rename_function(k): v for k, v in row.items()} for row in self.rows
        ]

    def to_dicts(self) -> List[Dict[str, Any]]:
        return self.rows
