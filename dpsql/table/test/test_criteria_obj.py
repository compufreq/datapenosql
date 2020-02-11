from dpsql.table.model.criterion_obj import AttrCriterion, CriterionClause, ValCriterion
from dpsql.table.model.db_objects import QueryTable, Column

tbl1 = QueryTable(name="test_table_1")
tbl2 = QueryTable(name="test_table_2")

col1 = Column(name="test_col_1", table=tbl1)
col2 = Column(name="test_col_2", table=tbl2)
col3 = Column(name="test_col_3", table=tbl2)
col4 = Column(name="test_col_4", table=tbl1)
col5 = Column(name="test_col_5", table=tbl1)


criteria1 = ValCriterion(left_table=tbl1, left_attr=col1, criteria=CriterionClause.Equal, values=[20])

criteria2 = AttrCriterion(left_table=tbl1, left_attr=col1, righ_table=tbl2, right_attr=col2, join_expression='(+)')
criteria2.clause_ = CriterionClause.Equal

criteria3 = AttrCriterion(left_table=tbl1, left_attr=col1, righ_table=tbl2, right_attr=col3, join_expression='(-)')
criteria3.clause_ = CriterionClause.Equal

criteria4 = ValCriterion(left_table=tbl1, left_attr=col4, criteria=CriterionClause.Equal, values=[30])

criteria5 = ValCriterion(left_table=tbl1, left_attr=col5, criteria=CriterionClause.Equal, values=[40])

crit_lst1 = list()
crit_lst1.append(criteria2)

crit_lst2 = list()
crit_lst2.append(criteria5)

crit_lst3 = list()
crit_lst3.append(criteria3)

criteria4.and_ = crit_lst2
criteria4.or_ = crit_lst3

crit_lst4 = list()
crit_lst4.append(criteria4)

criteria1.and_ = crit_lst1
criteria1.or_group_ = crit_lst4

print(criteria1)
