from dpsql.table.model.criterion_obj import ValCriterion, CriterionClause, Criterion
from dpsql.table.model.db_objects import QueryTable, Column, CustomColumn
from dpsql.table.model.select_obj import Select
from dpsql.table.model.statement_obj import Statement, StatementClause

res_table = QueryTable(name='Table1')
cols_req = {'id': Column(name="ID", table=res_table),
            'tb2_id': Column(name="TB2_ID", table=res_table),
            'day_date': Column(name="Day_Date", table=res_table),
            'year_month': Column(name="YEAR_MONTH", table=res_table),
            'col1_col1': Column(name="Col1", table=res_table)}
res_table.columns_ = cols_req

res_table2 = QueryTable(name='Table2')
cols_req2 = {'id': Column(name="ID", table=res_table2),
             'day_date': Column(name="Day_Date", table=res_table2),
             'year_month': Column(name="YEAR_MONTH", table=res_table2),
             'col1_col1': Column(name="Col1", table=res_table2)}
res_table2.columns_ = cols_req2

res_select = Select(
    tables=[res_table2], is_distinct=False,
    columns=[res_table2.columns_.get('id')],
    where=Statement(
        criteria=[
            ValCriterion(left_attr=res_table2.columns_.get('day_date'),
                         criteria=CriterionClause.Equal,
                         values=['12-12-2020']),
            ValCriterion(left_attr=res_table2.columns_.get('year_month'),
                         criteria=CriterionClause.Between,
                         values=['01-02-2020', '10-10-2020'])],
        clause=StatementClause.Where),
    group_by=Statement(
        criteria=[
            Criterion(left_attr=res_table2.columns_.get('day_date'))],
        clause=StatementClause.GroupBy)
)

res_select2 = Select(
    tables=[res_table], is_distinct=False,
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
                         values=['01-02-2020', '10-10-2020']),
            ValCriterion(left_attr=res_table.columns_.get('tb2_id'),
                         criteria=CriterionClause.IsIn,
                         values=[res_select])],
        clause=StatementClause.Where),
    group_by=Statement(
        criteria=[
            Criterion(left_attr=res_table.columns_.get('day_date'))],
        clause=StatementClause.GroupBy)
)

print(str(res_select2))
