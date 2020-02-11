from dpsql.table.model.case_obj import CaseObjectContent
from dpsql.table.model.criterion_obj import ValCriterion, CriterionClause
from dpsql.table.model.func_aggregate import AggregateFunc, AggregateClause
from dpsql.table.model.func_db import StrFunc, DBFunc, ExtractField
from dpsql.table.model.db_objects import QueryTable, Column, FuncColumn, CalculatedColumn

tbl1 = QueryTable(name="test_table_1")
tbl2 = QueryTable(name="test_table_2")

col1 = Column(name="test_col_1", table=tbl1)
col2 = Column(name="test_col_2", table=tbl2)
col3 = Column(name="test_col_3", table=tbl2)
col4 = Column(name="test_col_4", table=tbl1)
col5 = Column(name="test_col_5", table=tbl1)


str_func = StrFunc(columns=(col1,), alias='test_alias')
str_func.lpad_(5, '0')
print(str_func.func_str_)

extract_func = DBFunc(alias='test_extract_alias')
extract_func.extract_(column=col1, field=ExtractField.Year)
print(extract_func.func_str_)

criteria1 = ValCriterion(left_table=tbl1, left_attr=col1, criteria=CriterionClause.Equal, values=[40])

criteria2 = ValCriterion(left_table=tbl1, left_attr=col2, criteria=CriterionClause.Between, values=[40, 55])

case1 = CaseObjectContent(when=criteria1, then='2')
case2 = CaseObjectContent(when=criteria2, then='30')

case_func = DBFunc(alias='test_case_alias')
case_func.case_(cases=[case1, case2],)
print(case_func)

func_col = FuncColumn(alias="test_alias", db_function=case_func)
print(func_col)


func_col2 = FuncColumn(alias="test_alias", db_function=extract_func)
print(func_col2)


ag_func = AggregateFunc(is_func=True, calc_method='(x*y/z)')

calc_col3 = CalculatedColumn(alias="test_alias", db_calc=ag_func)
print(calc_col3)


ag_func2 = AggregateFunc(is_func=False, target=col1, aggregate_func=AggregateClause.MAX)

calc_col4 = CalculatedColumn(alias="test_alias", db_calc=ag_func2)
print(calc_col4)
