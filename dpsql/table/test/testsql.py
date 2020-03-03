from dpsql.table.model.criterion_obj import ValCriterion, CriterionClause, Criterion
from dpsql.table.model.db_objects import QueryTable, Column, CustomColumn
from dpsql.table.model.select_obj import Select
from dpsql.table.model.statement_obj import Statement, StatementClause

res_table = QueryTable(name='Table1')
cols_req = {'day_date': Column(name="Day_Date", table=res_table),
            'year_month': Column(name="YEAR_MONTH", table=res_table),
            'col1_col1': Column(name="Col1", table=res_table)}
res_table.columns_ = cols_req

res_select = Select(tables=[res_table], is_distinct=False,
                    custom_columns=[
                        CustomColumn(column=f"SUM({res_table.columns_.get('col1_col1')})",
                                     alias='column1')
                    ],
                    where=Statement(
                        criteria=[
                            ValCriterion(left_attr=res_table.columns_.get('day_date'),
                                         criteria=CriterionClause.Equal,
                                         values=['12-12-2020']),
                            ValCriterion(left_attr=res_table.columns_.get('year_month'),
                                         criteria=CriterionClause.Between,
                                         values=['01-02-2020', '10-10-2020'])],
                        clause=StatementClause.Where),
                    group_by=Statement(
                        criteria=[
                            Criterion(left_attr=res_table.columns_.get('day_date'))],
                        clause=StatementClause.GroupBy))

print(str(res_select))