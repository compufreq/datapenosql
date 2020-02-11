from dpsql.table.model.db_objects import QueryTable, Column
from dpsql.table.model.criterion_obj import AttrCriterion, CriterionClause, ValCriterion
from dpsql.table.model.db_objects import Join, JoinClause

tbl1 = QueryTable(name='test_table_1')
col1 = Column(name='test_col_1', table=tbl1)

tbl2 = QueryTable(name='test_table_2')
col2 = Column(name='test_col_1', table=tbl2)
col3 = Column(name='test_col_2', table=tbl2)

tbl3 = QueryTable(name='test_table_3')
col4 = Column(name='test_col_3', table=tbl3)
col5 = Column(name='test_col_4', table=tbl3)

criteria2 = ValCriterion(left_attr=col1, criteria=CriterionClause.Equal, values=[20])

criteria1 = AttrCriterion(left_attr=col1, right_attr=col2)
criteria1.clause_ = CriterionClause.Equal


criteria3 = AttrCriterion(left_attr=col2, right_attr=col3)
criteria3.clause_ = CriterionClause.Equal

criteria4 = AttrCriterion(left_attr=col1, right_attr=col5)
criteria4.clause_ = CriterionClause.Equal


join1 = Join(cross_tables=[tbl1, tbl2], where=[criteria1, criteria2], join_clause=JoinClause.Cross)
print(str(join1))

join2 = Join(cross_tables=[tbl1, tbl2], on=[criteria1, criteria2], join_clause=JoinClause.Cross)
print(str(join2))

join3 = Join(cross_tables=[tbl1, tbl3], where=[criteria3, criteria4], join_clause=JoinClause.Cross)
print(str(join3))

join4 = Join(left_table= tbl1, right_table=tbl2, on=[criteria1, criteria2], join_clause=JoinClause.Inner)
print(str(join4))