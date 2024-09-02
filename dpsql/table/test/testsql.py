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

res_table3 = QueryTable(name='Table3')
cols_req3 = {'id': Column(name="ID", table=res_table3),
             'tb2_id': Column(name="TB2_ID", table=res_table3),
             'day_date': Column(name="Day_Date", table=res_table3),
             'year_month': Column(name="YEAR_MONTH", table=res_table3),
             'col2_col2': Column(name="Col2", table=res_table3)}
res_table3.columns_ = cols_req3
res_table3_tb2_join = res_table3.inner_join_(
    res_table2,Statement(
        criteria=[
            ValCriterion(criterion_attr=res_table3.columns_.get('tb2_id'),
                               criteria=CriterionClause.Equal,
                               values=[res_table2.columns_.get('id')]),
            ValCriterion(criterion_attr=res_table3.columns_.get('day_date'),
                         criteria=CriterionClause.Equal,
                         values=[res_table2.columns_.get('day_date')])
        ]
    )
)

res_select = Select(
 tables=[res_table2], is_distinct=False,
 columns=[res_table2.columns_.get('id')],
 where=Statement(
     criteria=[
         ValCriterion(criterion_attr=res_table2.columns_.get('day_date'),
                      criteria=CriterionClause.Equal,
                      values=['12-12-2020']),
         ValCriterion(criterion_attr=res_table2.columns_.get('year_month'),
                      criteria=CriterionClause.Between,
                      values=['01-02-2020', '10-10-2020'])],
     clause=StatementClause.Where),
 group_by=Statement(
     criteria=[
         Criterion(criterion_attr=res_table2.columns_.get('day_date'))],
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
            ValCriterion(criterion_attr=res_table.columns_.get('day_date'),
                         criteria=CriterionClause.Equal,
                         values=['12-12-2020']),
            ValCriterion(criterion_attr=res_table.columns_.get('year_month'),
                         criteria=CriterionClause.Between,
                         values=['01-02-2020', '10-10-2020']),
            ValCriterion(criterion_attr=res_table.columns_.get('tb2_id'),
                         criteria=CriterionClause.IsIn,
                         values=[res_select])],
        clause=StatementClause.Where),
    group_by=Statement(
        criteria=[
            Criterion(left_attr=res_table.columns_.get('day_date'))],
        clause=StatementClause.GroupBy)
)

res_select3 = Select(
    join_query=res_table3_tb2_join.join_str_, is_distinct=False,
    custom_columns=[
        CustomColumn(column=f"SUM({res_table3.columns_.get('col2_col2')})",
                     alias='column1')
    ],
    where=Statement(
        criteria=[
            ValCriterion(criterion_attr=res_table3.columns_.get('day_date'),
                         criteria=CriterionClause.Equal,
                         values=['12-12-2020']),
            ValCriterion(criterion_attr=res_table3.columns_.get('year_month'),
                         criteria=CriterionClause.Between,
                         values=['01-02-2020', '10-10-2020']),
            ValCriterion(criterion_attr=res_table3.columns_.get('tb2_id'),
                         criteria=CriterionClause.IsIn,
                         values=[res_select])],
        clause=StatementClause.Where),
    group_by=Statement(
        criteria=[
            Criterion(criterion_attr=res_table3.columns_.get('day_date'))],
        clause=StatementClause.GroupBy)
)

print(str(res_select))

print(str(res_select2))

print(str(res_select3))