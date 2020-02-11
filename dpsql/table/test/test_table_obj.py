# -*- coding: utf-8 -*-
from dpsql.table.model.db_objects import QueryTable, Column

col1 = Column(name='test_col_1', alias='abc')
col2 = Column(name='test_col_2', alias='def')

tbl1 = QueryTable(name='test_table_1', alias='tbl1', columns=[col1, col2])

print(tbl1.column_name_getter())
