from dpsql.table.model.join_obj import QueryTable, Column
from dpsql.table.model.criterion_obj import AttrCriterion, CriterionClause, ValCriterion
from dpsql.table.model.join_obj import Join, JoinClause

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

criteria4 = AttrCriterion(left_attr=col1, right_attr=col5, criteria=CriterionClause.Equal)
criteria4.clause_ = CriterionClause.Equal

criteria5 = AttrCriterion(left_attr=col2, right_attr=col4)
criteria5.clause_ = CriterionClause.Equal

criteria6 = AttrCriterion(left_attr=col3, right_attr=col5, criteria=CriterionClause.Equal)
criteria6.clause_ = CriterionClause.Equal


t = tbl2.inner_join_(right_table=tbl3, on=[
    AttrCriterion(left_attr=col1, right_attr=col5, criteria=CriterionClause.Equal),
    AttrCriterion(left_attr=col3, right_attr=col5, criteria=CriterionClause.Equal)])\
    .left_outer_join_(right_table=tbl1, on=[criteria1, criteria2])\
    .inner_join_(right_table=tbl1, on=[criteria4, criteria3]).join_str_
print(t)
