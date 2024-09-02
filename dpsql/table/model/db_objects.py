from __future__ import annotations

import logging
from enum import Enum
from typing import List, Dict

from dpsql.table.model.func_aggregate import AggregateFunc
from dpsql.table.model.func_db import DBFunc
from dpsql.table.model.criterion_obj import AttrCriterion
from dpsql.table.model.statement_obj import Statement


class QueryCase(Enum):
    UpperCase = 1
    LowerCase = 2

class VariablesCase(Enum):
    UpperCase = 1
    LowerCase = 2
    StandardCase = 3

class QueryTable(object):
    def __init__(self, **kwargs):
        self._columns: Dict[Column]  = kwargs.get('columns', None)
        self._name: str = kwargs.get('name', '')
        self._alias: str = kwargs.get('alias', '')
        self._name_alias_system: str = kwargs.get('name_alias_system', '')
        #self._from_name: str = kwargs.get('from_name', '')
        self._join_str = ''
        self._query_case: QueryCase = kwargs.get('query_case', QueryCase.UpperCase)
        self._variables_case: VariablesCase = kwargs.get('variables_case', VariablesCase.StandardCase)

    def __str__(self):
        return self._name_getter()

    def __repr__(self):
        return self._name_getter()

    @property
    def columns_(self):
        return self._columns

    # @property
    # def from_name_(self):
    #     self._from_name = self.table_name_set_alias()
    #     return self._from_name

    @property
    def name_(self):
        return self._name

    @property
    def name_alias_system_(self):
        return self._name_alias_system

    @property
    def alias_(self):
        return self._alias

    @property
    def join_str_(self):
        return self._join_str

    @property
    def query_case_(self):
        return self._query_case

    @property
    def variables_case_(self):
        return self._variables_case


    @columns_.setter
    def columns_(self, param: List[Column]):
        self._columns = param

    # @from_name_.setter
    # def from_name_(self, param: str):
    #     self._from_name = param

    @name_alias_system_.setter
    def name_alias_system_(self, param: str):
        self._name_alias_system = param

    @name_.setter
    def name_(self, param: str):
        self._name = param

    @alias_.setter
    def alias_(self, param):
        self._alias = param
        self.name_alias_system_ = self._table_name_alias()

    @join_str_.setter
    def join_str_(self, param):
        self._join_str = param

    @query_case_.setter
    def query_case_(self, param):
        self._query_case = param

    @variables_case_.setter
    def variables_case_(self, param):
        self._variables_case = param


    def _table_name_alias(self):
        if self.alias_ != '':
            if self.variables_case_ == VariablesCase.UpperCase:
                self.alias_ = self.alias_.upper()
            elif self.variables_case_ == VariablesCase.LowerCase:
                self.alias_ = self.alias_.lower()
            else:
                self.alias_ = self.alias_
            return f"{self._name_getter()} {self.alias_}"
        else:
            return self._name_getter()

    def _name_getter(self):
        if self.variables_case_ == VariablesCase.UpperCase:
            return self.name_.upper()
        elif self.variables_case_ == VariablesCase.LowerCase:
            return self.name_.lower()
        else:
            return self.name_

    def _fill_return_join_str_result(self, right_table: QueryTable, res:str):
        if self.join_str_ == '':
            res = f"{self._table_name_alias()} {res}"
            self.join_str_ = res
            right_table.join_str_ = res
        else:
            self.join_str_ += res
            right_table.join_str_ = self.join_str_

        return right_table

    def inner_join_(self, right_table:QueryTable, on:Statement):
        logger = logging.getLogger(__name__)
        res = ''
        try:
            right_t = right_table._name_getter()
            if self.query_case_ == QueryCase.UpperCase:
                res = f"INNER JOIN {right_t} ON ("
                res += f"{' AND '.join(str(crit) for crit in on.criteria_)}) "
            else:
                res = f"inner join {right_t} on ("
                res += f"{' and '.join(str(crit) for crit in on.criteria_)}) "

            opt_table = self._fill_return_join_str_result(right_table, res)

            return opt_table

        except TypeError as error:
            logger.exception('The Join does not contain On attributes', exc_info=True)

    def left_outer_join_(self, right_table:QueryTable, on):
        logger = logging.getLogger(__name__)
        res = ''
        try:
            right_t = right_table._name_getter()
            if self.query_case_ == QueryCase.UpperCase:
                res = f"LEFT OUTER JOIN {right_t} ON ("
                res += f"{' AND '.join(str(crit) for crit in on)}) "
            else:
                res = f"left outer join {right_t} on ("
                res += f"{' and '.join(str(crit) for crit in on)}) "

            opt_table = self._fill_return_join_str_result(right_table, res)

            return opt_table

        except TypeError as error:
            logger.exception('The Join does not contain On attributes', exc_info=True)

    def right_outer_join_(self, right_table:QueryTable, on):
        logger = logging.getLogger(__name__)
        res = ''
        try:
            right_t = right_table._name_getter()
            if self.query_case_ == QueryCase.UpperCase:
                res = f"RIGHT OUTER JOIN {right_t} ON ("
                res += f"{' AND '.join(str(crit) for crit in on)}) "
            else:
                res = f"right outer join {right_t} on ("
                res += f"{' and '.join(str(crit) for crit in on)}) "

            opt_table = self._fill_return_join_str_result(right_table, res)

            return opt_table

        except TypeError as error:
            logger.exception('The Join does not contain On attributes', exc_info=True)

    def natural_join_(self, right_table:QueryTable, on):
        logger = logging.getLogger(__name__)
        res = ''
        try:

            right_t = right_table._name_getter()
            if self.query_case_ == QueryCase.UpperCase:
                res = f"NATURAL JOIN {right_t} ON ("
                res += f"{' AND '.join(str(crit) for crit in on)}) "
            else:
                res = f"natural join {right_t} on ("
                res += f"{' and '.join(str(crit) for crit in on)}) "

            opt_table = self._fill_return_join_str_result(right_table, res)

            return opt_table

        except TypeError as error:
            logger.exception('The Join does not contain On attributes', exc_info=True)

    def natural_left_join_(self, right_table:QueryTable, on):
        logger = logging.getLogger(__name__)
        res = ''
        try:

            right_t = right_table._name_getter()
            if self.query_case_ == QueryCase.UpperCase:
                res = f"NATURAL LEFT JOIN {right_t} ON ("
                res += f"{' AND '.join(str(crit) for crit in on)}) "
            else:
                res = f"natural left join {right_t} on ("
                res += f"{' and '.join(str(crit) for crit in on)}) "

            opt_table = self._fill_return_join_str_result(right_table, res)

            return opt_table

        except TypeError as error:
            logger.exception('The Join does not contain On attributes', exc_info=True)

    def natural_right_join_(self, right_table:QueryTable, on):
        logger = logging.getLogger(__name__)
        res = ''
        try:

            right_t = right_table._name_getter()
            if self.query_case_ == QueryCase.UpperCase:
                res = f"NATURAL RIGHT JOIN {right_t} ON ("
                res += f"{' AND '.join(str(crit) for crit in on)}) "
            else:
                res = f"natural right join {right_t} on ("
                res += f"{' and '.join(str(crit) for crit in on)}) "

            opt_table = self._fill_return_join_str_result(right_table, res)

            return opt_table

        except TypeError as error:
            logger.exception('The Join does not contain On attributes', exc_info=True)

    def column_name_getter(self):
        table_name = self._name_getter()
        res = f"{table_name}.*"
        if self.columns_:
            res = ''
            for column in self.columns_:
                if column.alias_ != '':
                    if res != '':
                        res += f', {table_name}.{column.name_} AS {column.alias_}'
                    else:
                        res += f'{table_name}.{column.name_} AS {column.alias_}'
                else:
                    if res != '':
                        res += f', {table_name}.{column.name_}'
                    else:
                        res += f'{table_name}.{column.name_}'
        return res


class Table(QueryTable):
    def __init__(self, **kwargs):
        super(Table, self).__init__(**kwargs)
        self._primary_columns: List[PrimaryColumn] = kwargs.get('primary_columns', None)
        self._foreign_columns: List[RefColumn] = kwargs.get('foreign_columns', None)

    @property
    def primary_columns_(self):
        return self._primary_columns

    @property
    def foreign_columns_(self):
        return self._foreign_columns

    @primary_columns_.setter
    def primary_columns_(self, param: List[PrimaryColumn]):
        self._primary_columns = param

    @foreign_columns_.setter
    def foreign_columns_(self, param: List[RefColumn]):
        self._foreign_columns = param


class Column(object):
    def __init__(self, **kwargs):
        self._name: str = kwargs.get('name', '')
        self._alias: str = kwargs.get('alias', '')
        self._type: str = kwargs.get('type', '')
        self._col_str: str = ''
        self._table_name: str = kwargs.get('table', '')

    @property
    def name_(self):
        return self._name

    @property
    def alias_(self):
        return self._alias

    @property
    def type_(self):
        return self._type

    @property
    def col_str_(self):
        return self._col_str

    @property
    def table_name_(self):
        return self._table_name

    @name_.setter
    def name_(self, param):
        self._name = param

    @alias_.setter
    def alias_(self, param):
        self._alias = param

    @type_.setter
    def type_(self, param):
        self._type = param

    @col_str_.setter
    def col_str_(self, param):
        self._col_str = param

    @table_name_.setter
    def table_name_(self, param):
        self._table_name = param

    def __str__(self):
        self.col_str_ = self.table_col_name()
        return self.col_str_

    def table_col_name(self) -> str:
        if self.table_name_ != '':
            return f'{self.table_name_}.{self.name_}'

    def set_col_alias(self):
        attrib_alias = self.alias_ if self.alias_ != "" else ""
        if attrib_alias != "":
            return f'{self.table_col_name()}  {attrib_alias}'
        else:
            return str(self)


class RefColumn(Column):
    def __init__(self, **kwargs):
        super(RefColumn, self).__init__(**kwargs)
        self._foreign_table: QueryTable = kwargs.get('foreign_table', None)

    @property
    def foreign_table_(self):
        return self._foreign_table

    @foreign_table_.setter
    def foreign_table_(self, param):
        self._foreign_table = param


class PrimaryColumn(Column):
    def __init__(self, **kwargs):
        super(PrimaryColumn, self).__init__(**kwargs)
        self._is_identity: bool = kwargs.get('is_identity', False)
        self._increment_factor: int = kwargs.get('increment_factor', 0)

    @property
    def is_identity(self):
        return self._is_identity

    @property
    def increment_factor(self):
        return self._increment_factor

    @is_identity.setter
    def is_identity(self, param):
        self._is_identity = param

    @increment_factor.setter
    def increment_factor(self, param):
        self._increment_factor = param


class FuncColumn(Column):
    def __init__(self, **kwargs):
        super(FuncColumn, self).__init__(**kwargs)
        self._db_func: DBFunc = kwargs.get('db_function', None)

    def __str__(self):
        if self.alias_ != '':
            self.col_str_ = f'({str(self._db_func)}) {self.alias_}'
        return self.col_str_

    @property
    def db_func_(self):
        return self._db_func

    @db_func_.setter
    def db_func_(self, param):
        self._db_func = param


class CalculatedColumn(Column):
    def __init__(self, **kwargs):
        super(CalculatedColumn, self).__init__(**kwargs)
        self._db_calc: AggregateFunc = kwargs.get('db_calc', None)

    def __str__(self):
        if self.alias_ != '':
            self.col_str_ = f'{str(self._db_calc)} {self.alias_}'
        return self.col_str_

    @property
    def db_calc_(self):
        return self._db_calc

    @db_calc_.setter
    def db_calc_(self, param):
        self._db_calc = param


class CustomColumn(object):
    def __init__(self, **kwargs):
        self._column: str = kwargs.get('column', '')
        self._alias: str =  kwargs.get('alias', '')

    def __str__(self):
        if self.alias_ != '':
            self.col_str_ = f'{str(self._column).rstrip()} {self.alias_}'
        return self.col_str_

    @property
    def column_(self):
        return self._column

    @property
    def alias_(self):
        return self._alias

    @column_.setter
    def column_(self, param):
        self._column = param

    @alias_.setter
    def alias_(self, param):
        self._alias = param
