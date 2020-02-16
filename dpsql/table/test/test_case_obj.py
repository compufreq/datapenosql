from dpsql.table.model.case_obj import CaseObjectContent
from dpsql.table.model.criterion_obj import CriterionClause, ValCriterion
from dpsql.table.model.db_objects import QueryTable, Column

tbl1 = QueryTable(name="test_table_1")
tbl2 = QueryTable(name="test_table_2")

col1 = Column(name="test_col_1", table=tbl1)
col2 = Column(name="test_col_2", table=tbl2)
col3 = Column(name="test_col_3", table=tbl2)
col4 = Column(name="test_col_4", table=tbl1)
col5 = Column(name="test_col_5", table=tbl1)

criteria1 = ValCriterion(left_table=tbl1, left_attr=col1, criteria=CriterionClause.Equal, values=[40])

criteria2 = ValCriterion(left_table=tbl2, left_attr=col2, criteria=CriterionClause.Between, values=[40, 55])

case1 = CaseObjectContent(when=criteria1, then='2')
case2 = CaseObjectContent(when=criteria2, then='30')
