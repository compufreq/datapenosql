from typing import List

from dpsql.table.model.db_objects import Column, QueryTable
from dpsql.table.model.db_objects import Join
from dpsql.table.model.statement_obj import Statement


class Select(object):
    def __init__(self, **kwargs):
        self._tables = str
        self._columns: List[Column] = kwargs.get('cols', '')
        self._calculated_column: List = None
        self._aggregated_column: List = None
        self._column_str: str = ''
        self._calculated_column_str: str = ''
        self._aggregated_column_str: str = ''
        self._joins: str = kwargs.get('joins', '')
        self._joins_str: str = ''
        self._where: str = kwargs.get('where', '')
        self._where_str: str = ''
        self._group_by: str = kwargs.get('group_by', '')
        self._group_by_str: str = ''
        self._having: str = kwargs.get('having', '')
        self._having_str: str = ''
        self._order_by: str = kwargs.get('order_by', '')
        self._order_by_str: str = ''
        self._is_attrib: bool = kwargs.get('is_attrib', False)
        self._is_sub_query: bool = kwargs.get('is_sub_query', False)
        self._alias: str = kwargs.get('alias', '')
        self._select_str: str = kwargs.get('select_query', '')

    def __str__(self):
        self._select_builder()
        return self.select_str_

    def __repr__(self):
        self._select_builder()
        return self.select_str_

    @property
    def tables_(self):
        return self._tables

    @property
    def columns_(self):
        return self._columns

    @property
    def calculated_column_(self):
        return self._calculated_column

    @property
    def aggregated_column_(self):
        return self._aggregated_column

    @property
    def joins_(self):
        return self._joins

    @property
    def where_(self):
        return self._where

    @property
    def group_by_(self):
        return self._group_by

    @property
    def having_(self):
        return self._having

    @property
    def order_by_(self):
        return self._order_by

    @property
    def is_attrib_(self):
        return self._is_attrib

    @property
    def is_sub_query_(self):
        return self._is_sub_query

    @property
    def alias_(self):
        return self._alias

    @property
    def select_str_(self):
        return self._select_str

    @tables_.setter
    def tables_(self, param):
        self._tables = param

    @columns_.setter
    def columns_(self, param):
        self._columns = param

    @calculated_column_.setter
    def calculated_column_(self, param):
        self._calculated_column = param

    @aggregated_column_.setter
    def aggregated_column_(self, param):
        self._aggregated_column = param

    @joins_.setter
    def joins_(self, param):
        self._joins = param

    @where_.setter
    def where_(self, param):
        self._where = param

    @group_by_.setter
    def group_by_(self, param):
        self._group_by = param

    @having_.setter
    def having_(self, param):
        self._having = param

    @order_by_.setter
    def order_by_(self, param):
        self._order_by = param

    @is_attrib_.setter
    def is_attrib_(self, param):
        self._is_attrib = param

    @is_sub_query_.setter
    def is_sub_query_(self, param):
        self._is_sub_query = param

    @alias_.setter
    def alias_(self, param):
        self._alias = param

    @select_str_.setter
    def select_str_(self, param):
        self._select_str = param

    def query_alias(self) -> str:
        if self.alias_ != "" and self.is_attrib_ is True:
            return f'({self.select_str_}) AS {self.alias_}'
        
        elif self.alias_ != "" and self.is_sub_query_ is True:
            return f'({self.select_str_}) {self.alias_}'
        
        else:
            return self.select_str_

    def _columns_builder(self):
        if self.columns_:
            self._column_str = f"{', '.join(str(column) for column in self.columns_)}"

    def _calculated_columns_builder(self):
        if self.columns_:
            self._calculated_column_str = f"{', '.join(str(column) for column in self.calculated_column_)}"

    def _aggregated_columns_builder(self):
        if self.columns_:
            self._aggregated_column_str = f"{', '.join(str(column) for column in self.aggregated_column_)}"

    def _select_builder(self):
        if self.columns_:
            self._columns_builder()
        if self.calculated_column_:
            self._calculated_columns_builder()
        if self._aggregated_column:
            self._aggregated_columns_builder()

        columns = self._column_str if self._column_str != '' else '*'
        calc_columns = self._calculated_column_str if self._calculated_column_str != '' else ''
        aggr_columns = self._aggregated_column_str if self._aggregated_column_str != '' else ''

        columns_combiner = ''
        if calc_columns != '' and aggr_columns != '':
            columns_combiner = f"{columns}, {calc_columns}, {aggr_columns}"
        elif calc_columns != '' and aggr_columns == '':
            columns_combiner = f"{columns}, {calc_columns}"
        elif calc_columns == '' and aggr_columns != '':
            columns_combiner = f"{columns}, {aggr_columns}"
        else:
            columns_combiner = columns

        select_temp = f"Select {columns_combiner}"

        if self.joins_ != '':
            select_temp += f" From {self.joins_}"
        else:
            select_temp += f" From {self.tables_}"

        if self.where_ != '':
            select_temp += f" {self.where_}"

        if self.group_by_ != '':
            select_temp += f" {self.group_by_}"

            if self.having_ != '':
                select_temp += f" {self.having_}"

            if self.order_by_ != '':
                select_temp += f" {self.order_by_}"

        elif self.having_ != '':
            select_temp += f" {self.having_}"

            if self.order_by_ != '':
                select_temp += f" {self.order_by_}"

        elif self.order_by_ != '':
            select_temp += f" {self.order_by_}"

        if self.is_sub_query_:
            select_temp = f"({select_temp}) {self.alias_}"

        self.select_str_ = select_temp

        return self.select_str_