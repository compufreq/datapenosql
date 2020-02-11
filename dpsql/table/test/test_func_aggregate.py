from dpsql.table.model.db_objects import QueryTable, Column
from dpsql.table.model.func_aggregate import AggregateFunc, AggregateClause

tbl1 = QueryTable(name="test_table_1")
tbl2 = QueryTable(name="test_table_2")

col1 = Column(name="day_date", table=tbl1)
col2 = Column(name="salary", table=tbl2)
col3 = Column(name="employees", table=tbl2)

test1 = AggregateFunc(target=col1, alias="Max_Date", aggregate_func=AggregateClause.MAX)
print(test1)

test2 = AggregateFunc(target=col1, alias="Min_Date", aggregate_func=AggregateClause.MIN)
print(test2)

test3 = AggregateFunc(target=col2, alias="AVG_Tot", aggregate_func=AggregateClause.AVG)
print(test3)


test4 = AggregateFunc(target=col2, alias="Sum_Tot", aggregate_func=AggregateClause.SUM)
print(test4)

test5 = AggregateFunc(target=col3, alias="Count_Tot", aggregate_func=AggregateClause.COUNT)
print(test5)
