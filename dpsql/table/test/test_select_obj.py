# -*- coding: utf-8 -*-
from dpsql.table.model.case_obj import CaseObjectContent
from dpsql.table.model.criterion_obj import ValCriterion, CriterionClause, AttrCriterion
from dpsql.table.model.db_objects import QueryTable, Column, Join, JoinClause, CustomColumn
from dpsql.table.model.func_db import StrFunc, DBFunc
from dpsql.table.model.select_obj import Select
from dpsql.table.model.statement_obj import Statement, StatementClause

sel_obj = Select()

# -------------------- Tables Definition Section -------------------- #
customer = QueryTable(name="customer@bankbi", alias='a')
customer_columns = {
    'cus_num': Column(name="cus_num", table=customer),
    'bra_code': Column(name="bra_code", table=customer),
    'tit_code': Column(name="TIT_CODE", table=customer),
    'nat_num': Column(name="nat_num", table=customer),
    'id_type': Column(name="id_type", table=customer),
    'bir_date': Column(name="bir_date", table=customer),
    'birth_place': Column(name="birth_place", table=customer),
    'cus_class': Column(name="cus_class", table=customer),
    'cou_of_resi': Column(name="cou_of_resi", table=customer)
}
customer.columns_ = customer_columns

account = QueryTable(name="account@bankbi", alias='b')
account_columns = {
    'led_code':Column(name="led_code", table=account, alias='led_code'),
    'cur_code':Column(name="cur_code", table=account),
    'cus_num': Column(name="cus_num", table=account),
    'bra_code': Column(name="bra_code", table=account),
    'sta_code':Column(name="STA_CODE", table=account),
    'sub_acct_code':Column(name="sub_acct_code", table=account),
    'date_open':Column(name="date_open", table=account)
}
account.columns_ = account_columns

address = QueryTable(name="address@bankbi", alias='c')
address_columns = {
    'led_code':Column(name="led_code", table=address),
    'cus_num': Column(name="cus_num", table=address),
    'bra_code':Column(name="bra_code", table=address),
    'lan_ind':Column(name="lan_ind", table=address),
    'name_line1':Column(name="name_line1", table=address),
    'name_line2':Column(name="name_line2", table=address),
    'add_line1':Column(name="add_line1", table=address, alias='address_1'),
    'add_line2':Column(name="add_line2", table=address, alias='address_2'),
    'city_loc_code':Column(name="city_loc_code", table=address),
    'street':Column(name="street", table=address),
    'po_box_num':Column(name="po_box_num", table=address),
    'postcode':Column(name="postcode", table=address),
    'tel_num':Column(name="tel_num", table=address),
    'mob_num':Column(name="mob_num", table=address),
    'email':Column(name="email", table=address),
    'fax_num':Column(name="fax_num", table=address)
}
address.columns_ = address_columns

oth_add = QueryTable(name="oth_add@bankbi", alias='d')
oth_add_columns = {
    'led_code':Column(name="led_code", table=oth_add),
    'cus_num': Column(name="cus_num", table=oth_add),
    'bra_code':Column(name="bra_code", table=oth_add),
    'name_line1':Column(name="name_line1", table=oth_add),
    'name_line2':Column(name="name_line2", table=oth_add),
    'add_line1':Column(name="add_line1", table=oth_add, alias='other_address_1'),
    'add_line2':Column(name="add_line2", table=oth_add, alias='other_address_2')
}
oth_add.columns_ = oth_add_columns

chrt_act = QueryTable(name="chrt_act@bankbi", alias='e')
chrt_act_columns = {
    'led_code':Column(name="led_code", table=chrt_act),
    'des_eng':Column(name="des_eng", table=chrt_act, alias='led_desc')
}
chrt_act.columns_ = chrt_act_columns

currency = QueryTable(name="currency@bankbi", alias='f')
currency_columns = {
    'cur_code':Column(name="cur_code", table=currency),
    'des_eng':Column(name="des_eng", table=currency, alias='cur_desc')
}
currency.columns_ = currency_columns

cust_pro = QueryTable(name="cust_pro@bankbi", alias='g')
cust_pro_columns = {
    'cus_num':Column(name="cus_num", table=cust_pro),
    'bra_code':Column(name="bra_code", table=cust_pro),
    'first_name':Column(name="first_name", table=cust_pro),
    'father_name_1':Column(name="father_name_1", table=cust_pro),
    'father_name_2':Column(name="father_name_2", table=cust_pro),
    'gfather_name':Column(name="gfather_name", table=cust_pro),
    'last_name':Column(name="last_name", table=cust_pro),
    'gender':Column(name="gender", table=cust_pro),
    'mrtl_sta':Column(name="mrtl_sta", table=cust_pro),
    'monthly_sal':Column(name="monthly_sal", table=cust_pro),
    'ann_sal':Column(name="ann_sal", table=cust_pro)
}
cust_pro.columns_ = cust_pro_columns

ftca_tab = QueryTable(name="ftca_tab@bankbi", alias='h')
ftca_tab_columns = {
    'cus_num':Column(name="cus_num", table=ftca_tab),
    'iris_form_req':Column(name="iris_form_req", table=ftca_tab)
}
ftca_tab.columns_ = ftca_tab_columns

text_tab = QueryTable(name="TEXT_TAB@BANKBI")
text_tab_columns = {
    'tab_id':Column(name="TAB_ID", table=text_tab),
    'tab_ent':Column(name="TAB_ENT", table=text_tab),
    'des_eng':Column(name="DES_ENG", table=text_tab)
}
text_tab.columns_ = text_tab_columns


# -------------------- Customer account_number Concat LPAD Functions Results -------------------- #
account_number = StrFunc(
    columns=[
    str(StrFunc(columns=(account.columns_.get('bra_code'),)).lpad_(4, '0')),
    str(StrFunc(columns=(account.columns_.get('cus_num'),)).lpad_(7, '0')),
    str(StrFunc(columns=(account.columns_.get('cur_code'),)).lpad_(3, '0')),
    str(StrFunc(columns=(account.columns_.get('led_code'),)).lpad_(4, '0')),
    str(StrFunc(columns=(account.columns_.get('sub_acct_code'),)).lpad_(3, '0'))]
).concat_()

# -------------------- Customer name_ar Concat LPAD Functions Results -------------------- #
address_tbl_name = StrFunc(
    columns=[
        str(address.columns_.get('name_line1')),
        f"' '",
        str(address.columns_.get('name_line2'))]
).concat_()


oth_add_tbl_name = StrFunc(
    columns=[
        str(oth_add.columns_.get('name_line1')),
        f"' '",
        str(oth_add.columns_.get('name_line2'))]
).concat_()

c_lan_ind_criterion1 = ValCriterion(left_attr=address.columns_.get('lan_ind'), criteria=CriterionClause.Equal,
                                    values=[1])
c_lan_ind_criterion2 = ValCriterion(left_attr=address.columns_.get('lan_ind'), criteria=CriterionClause.Equal,
                                    values=[2])

name_ar_cases = DBFunc().case_(
    cases=[
        CaseObjectContent(when=c_lan_ind_criterion1, then=str(oth_add_tbl_name)),
        CaseObjectContent(when=c_lan_ind_criterion2, then=str(address_tbl_name))]
)

# -------------------- Customer name_en Concat LPAD Functions Results -------------------- #

name_en_cases = DBFunc().case_(
    cases=[
        CaseObjectContent(when=c_lan_ind_criterion1, then=str(address_tbl_name)),
        CaseObjectContent(when=c_lan_ind_criterion2, then=str(oth_add_tbl_name))]
)


# -------------------- Customer middle_name Concat Function Results -------------------- #
cust_pro_tbl_mid_name = StrFunc(
    columns=[
        str(cust_pro.columns_.get('father_name_1')),
        f"' '",
        str(cust_pro.columns_.get('father_name_2')),
        f"' '",
        str(cust_pro.columns_.get('gfather_name'))]
).concat_()

# -------------------- Gender Decode Function Results -------------------- #
cust_pro_gender = StrFunc(
    columns=(cust_pro.columns_.get('gender'),)
).decode_(
    [
        (0, f"'Female'"),
        (1, f"'Male'"),
        (2, f"'Not Defined'"),
        (f"'No Data'",)]
)

# -------------------- Customer Title Sub Select -------------------- #
cus_title_select = Select(
    is_sub_query= True,
    tables= [text_tab],
    columns=
    [
        text_tab.columns_.get('des_eng')
    ],
    where=
    Statement(
        criteria=[
            ValCriterion(
                left_attr=text_tab.columns_.get('tab_id'),
                criteria=CriterionClause.Equal, values=[90],
                and_=[
                    ValCriterion(
                        left_attr=text_tab.columns_.get('tab_ent'),
                        criteria=CriterionClause.Equal,
                        values=[customer.columns_.get('tit_code')]
                )]
            )],
        clause=StatementClause.Where)
)

# -------------------- ID_TYPE Sub Select -------------------- #
cus_id_type_select = Select(
    is_sub_query= True,
    tables= [text_tab],
    columns=
    [
        text_tab.columns_.get('des_eng')
    ],
    where=
    Statement(
        criteria=[
            ValCriterion(
                left_attr=text_tab.columns_.get('tab_id'),
                criteria=CriterionClause.Equal, values=[98],
                and_=[
                    ValCriterion(
                        left_attr=text_tab.columns_.get('tab_ent'),
                        criteria=CriterionClause.Equal,
                        values=[customer.columns_.get('id_type')]
                )]
            )],
        clause=StatementClause.Where)
)

# -------------------- MARITAL_STATUS Sub Select -------------------- #
marital_status_select = Select(
    is_sub_query= True,
    tables= [text_tab],
    columns=
    [
        text_tab.columns_.get('des_eng')
    ],
    where=
    Statement(
        criteria=[
            ValCriterion(
                left_attr=text_tab.columns_.get('tab_id'),
                criteria=CriterionClause.Equal, values=[647],
                and_=[
                    ValCriterion(
                        left_attr=text_tab.columns_.get('tab_ent'),
                        criteria=CriterionClause.Equal,
                        values=[cust_pro.columns_.get('mrtl_sta')]
                )]
            )],
        clause=StatementClause.Where)
)

# -------------------- CUS_CLASS Sub Select -------------------- #
cus_class_select = Select(
    is_sub_query= True,
    tables= [text_tab],
    columns=
    [
        text_tab.columns_.get('des_eng')
    ],
    where=
    Statement(
        criteria=[
            ValCriterion(
                left_attr=text_tab.columns_.get('tab_id'),
                criteria=CriterionClause.Equal, values=[55],
                and_=[
                    ValCriterion(
                        left_attr=text_tab.columns_.get('tab_ent'),
                        criteria=CriterionClause.Equal,
                        values=[customer.columns_.get('cus_class')]
                )]
            )],
        clause=StatementClause.Where)
)

# -------------------- IRIS Sub Select -------------------- #
cus_iris_select = Select(
    is_sub_query= True,
    tables= [text_tab],
    columns=
    [
        text_tab.columns_.get('des_eng')
    ],
    where=
    Statement(
        criteria=[
            ValCriterion(
                left_attr=text_tab.columns_.get('tab_id'),
                criteria=CriterionClause.Equal, values=[1670],
                and_=[
                    ValCriterion(
                        left_attr=text_tab.columns_.get('tab_ent'),
                        criteria=CriterionClause.Equal,
                        values=[ftca_tab.columns_.get('iris_form_req')]
                )]
            )],
        clause=StatementClause.Where)
)

# -------------------- CITY Select -------------------- #
city_select = Select(
    is_sub_query= True,
    tables= [text_tab],
    columns=
    [
        text_tab.columns_.get('des_eng')
    ],
    where=
    Statement(
        criteria=[
            ValCriterion(
                left_attr=text_tab.columns_.get('tab_id'),
                criteria=CriterionClause.Equal, values=[190],
                and_=[
                    ValCriterion(
                        left_attr=text_tab.columns_.get('tab_ent'),
                        criteria=CriterionClause.Equal,
                        values=[address.columns_.get('city_loc_code')]
                )]
            )],
        clause=StatementClause.Where)
)


# -------------------- COU_OF_RES Select -------------------- #
cou_of_res_select = Select(
    is_sub_query= True,
    tables= [text_tab],
    columns=
    [
        text_tab.columns_.get('des_eng')
    ],
    where=
    Statement(
        criteria=[
            ValCriterion(
                left_attr=text_tab.columns_.get('tab_id'),
                criteria=CriterionClause.Equal, values=[10],
                and_=[
                    ValCriterion(
                        left_attr=text_tab.columns_.get('tab_ent'),
                        criteria=CriterionClause.Equal,
                        values=[customer.columns_.get('cou_of_resi')]
                )]
            )],
        clause=StatementClause.Where)
)

# -------------------- ACCT_STATUS Select -------------------- #
acc_status_select = Select(
    is_sub_query= True,
    tables= [text_tab],
    columns=
    [
        text_tab.columns_.get('des_eng')
    ],
    where=
    Statement(
        criteria=[
            ValCriterion(
                left_attr=text_tab.columns_.get('tab_id'),
                criteria=CriterionClause.Equal, values=[5],
                and_=[
                    ValCriterion(
                        left_attr=text_tab.columns_.get('tab_ent'),
                        criteria=CriterionClause.Equal,
                        values=[account.columns_.get('sta_code')]
                )]
            )],
        clause=StatementClause.Where)
)

# -------------------- Required Select -------------------- #

final_select = Select(
    is_sub_query= False,
    is_distinct=True,
    tables=[customer, account, address, oth_add, chrt_act, currency, cust_pro, ftca_tab],
    columns=[
        customer.columns_.get('bra_code'),
        customer.columns_.get('cus_num'),
        cust_pro.columns_.get('first_name'),
        cust_pro.columns_.get('last_name'),
        customer.columns_.get('nat_num'),
        customer.columns_.get('bir_date'),
        customer.columns_.get('birth_place'),
        cust_pro.columns_.get('monthly_sal'),
        cust_pro.columns_.get('ann_sal'),
        address.columns_.get('add_line1'),
        address.columns_.get('add_line2'),
        oth_add.columns_.get('add_line1'),
        oth_add.columns_.get('add_line2'),
        address.columns_.get('street'),
        address.columns_.get('po_box_num'),
        address.columns_.get('postcode'),
        address.columns_.get('tel_num'),
        address.columns_.get('mob_num'),
        address.columns_.get('email'),
        address.columns_.get('fax_num'),
        account.columns_.get('date_open'),
        account.columns_.get('led_code'),
        chrt_act.columns_.get('des_eng'),
        currency.columns_.get('des_eng')
    ],
    custom_columns=[
        CustomColumn(column=account_number, alias='accountnumber'),
        CustomColumn(column=f"({name_ar_cases})", alias='name_ar'),
        CustomColumn(column=f"({name_en_cases})", alias='name_en'),
        CustomColumn(column=cus_title_select, alias='CUS_TITLE'),
        CustomColumn(column=f"({cust_pro_tbl_mid_name})", alias='MIDDLE_NAME'),
        CustomColumn(column=cust_pro_gender, alias='gender'),
        CustomColumn(column=cus_id_type_select, alias='ID_TYPE'),
        CustomColumn(column=marital_status_select, alias='MARITAL_STATUS'),
        CustomColumn(column=cus_class_select, alias='CUS_CLASS'),
        CustomColumn(column=cus_iris_select, alias='IRIS'),
        CustomColumn(column=city_select, alias='CITY'),
        CustomColumn(column=cou_of_res_select, alias='COU_OF_RES'),
        CustomColumn(column=acc_status_select, alias='ACCT_STATUS')
    ],
    where=
    Statement(
        criteria=[
            AttrCriterion(left_attr=account.columns_.get('led_code'), right_attr=chrt_act.columns_.get('led_code')),
            AttrCriterion(left_attr=account.columns_.get('cur_code'), right_attr=currency.columns_.get('cur_code')),
            AttrCriterion(left_attr=customer.columns_.get('cus_num'), right_attr=account.columns_.get('cus_num')),
            AttrCriterion(left_attr=customer.columns_.get('cus_num'), right_attr=address.columns_.get('cus_num')),
            AttrCriterion(left_attr=oth_add.columns_.get('cus_num'), right_attr=customer.columns_.get('cus_num'),
                          join_expression='(+)'),
            AttrCriterion(left_attr=account.columns_.get('cus_num'), right_attr=address.columns_.get('cus_num')),
            AttrCriterion(left_attr=customer.columns_.get('bra_code'), right_attr=account.columns_.get('bra_code')),
            AttrCriterion(left_attr=customer.columns_.get('bra_code'), right_attr=address.columns_.get('bra_code')),
            AttrCriterion(left_attr=oth_add.columns_.get('bra_code'), right_attr=customer.columns_.get('bra_code'),
                          join_expression='(+)'),
            AttrCriterion(left_attr=account.columns_.get('bra_code'), right_attr=address.columns_.get('bra_code')),
            AttrCriterion(left_attr=customer.columns_.get('cus_num'), right_attr=cust_pro.columns_.get('cus_num')),
            AttrCriterion(left_attr=customer.columns_.get('bra_code'), right_attr=cust_pro.columns_.get('bra_code')),
            AttrCriterion(left_attr=ftca_tab.columns_.get('cus_num'), right_attr=customer.columns_.get('cus_num'),
                          join_expression='(+)'),
            ValCriterion(left_attr=customer.columns_.get('cus_num'), criteria=CriterionClause.Equal, values=[1]),
            ValCriterion(left_attr=account.columns_.get('sta_code'), criteria=CriterionClause.Equal, values=[1]),
            ValCriterion(left_attr=account.columns_.get('cur_code'), criteria=CriterionClause.Equal, values=[1]),
            ValCriterion(left_attr=account.columns_.get('led_code'), criteria=CriterionClause.IsIn,
                         values=[3000, 3001]),
            ValCriterion(left_attr=address.columns_.get('led_code'), criteria=CriterionClause.Equal, values=[0]),
            ValCriterion(left_attr=oth_add.columns_.get('led_code'), criteria=CriterionClause.Equal, values=[0])
        ],
        clause=StatementClause.Where)
)

print(final_select)