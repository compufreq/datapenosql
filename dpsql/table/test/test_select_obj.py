# -*- coding: utf-8 -*-
from dpsql.table.model.case_obj import CaseObjectContent
from dpsql.table.model.criterion_obj import ValCriterion, CriterionClause, AttrCriterion
from dpsql.table.model.db_objects import QueryTable, Column, Join, JoinClause
from dpsql.table.model.func_db import StrFunc, DBFunc
from dpsql.table.model.select_obj import Select
from dpsql.table.model.statement_obj import Statement, StatementClause

sel_obj = Select()

# -------------------- Tables Definition Section -------------------- #
customer = QueryTable(name="customer@bankbi", alias='a')
account = QueryTable(name="account@bankbi", alias='b')
address = QueryTable(name="address@bankbi", alias='c')
oth_add = QueryTable(name="oth_add@bankbi", alias='d')
chrt_act = QueryTable(name="chrt_act@bankbi", alias='e')
currency = QueryTable(name="currency@bankbi", alias='f')
cust_pro = QueryTable(name="cust_pro@bankbi", alias='g')
ftca_tab = QueryTable(name="ftca_tab@bankbi", alias='h')
text_tab = QueryTable(name="TEXT_TAB@BANKBI")

# -------------------- text_tab Table Columns -------------------- #
tab_id = Column(name="TAB_ID", table=text_tab)
tab_ent = Column(name="TAB_ENT", table=text_tab)
des_eng = Column(name="DES_ENG", table=text_tab)


# -------------------- customer Table Columns -------------------- #
a_cus_num = Column(name="cus_num", table=customer)
a_bra_code = Column(name="bra_code", table=customer)
a_tit_code = Column(name="TIT_CODE", table=customer)
a_nat_num = Column(name="nat_num", table=customer)
a_id_type = Column(name="id_type", table=customer)
a_bir_date = Column(name="bir_date", table=customer)
a_birth_place = Column(name="birth_place", table=customer)
a_cus_class = Column(name="cus_class", table=customer)
a_cou_of_resi = Column(name="cou_of_resi", table=customer)

# -------------------- account Table Columns -------------------- #
b_led_code = Column(name="led_code", table=account, alias='led_code')
b_cur_code = Column(name="cur_code", table=account)
b_cus_num = Column(name="led_code", table=account)
b_bra_code = Column(name="cur_code", table=account)
b_sta_code = Column(name="STA_CODE", table=account)
b_sub_acct_code = Column(name="sub_acct_code", table=account)
b_date_open = Column(name="date_open", table=account)

# -------------------- address Table Columns -------------------- #
c_led_code = Column(name="led_code", table=address)
c_cus_num = Column(name="led_code", table=address)
c_bra_code = Column(name="cur_code", table=address)
c_lan_ind = Column(name="lan_ind", table=address)
c_name_line1 = Column(name="name_line1", table=address)
c_name_line2 = Column(name="name_line2", table=address)
c_add_line1 = Column(name="add_line1", table=address, alias='address_1')
c_add_line2 = Column(name="add_line2", table=address, alias='address_2')
c_city_loc_code = Column(name="city_loc_code", table=address)
c_street = Column(name="street", table=address)
c_po_box_num = Column(name="po_box_num", table=address)
c_postcode = Column(name="postcode", table=address)
c_tel_num = Column(name="tel_num", table=address)
c_mob_num = Column(name="mob_num", table=address)
c_email = Column(name="email", table=address)
c_fax_num = Column(name="fax_num", table=address)

# -------------------- oth_add Table Columns -------------------- #
d_led_code = Column(name="led_code", table=oth_add)
d_cus_num = Column(name="led_code", table=oth_add)
d_bra_code = Column(name="cur_code", table=oth_add)
d_name_line1 = Column(name="name_line1", table=oth_add)
d_name_line2 = Column(name="name_line2", table=oth_add)
d_add_line1 = Column(name="add_line1", table=oth_add, alias='other_address_1')
d_add_line2 = Column(name="add_line2", table=oth_add, alias='other_address_2')

# -------------------- chrt_act Table Columns -------------------- #
e_led_code = Column(name="led_code", table=chrt_act)
e_des_eng = Column(name="des_eng", table=chrt_act, alias='led_desc')

# -------------------- currency Table Columns -------------------- #
f_cur_code = Column(name="cur_code", table=currency)
f_des_eng = Column(name="des_eng", table=currency, alias='cur_desc')

# -------------------- cust_pro Table Columns -------------------- #
g_cus_num = Column(name="cus_num", table=cust_pro)
g_bra_code = Column(name="bra_code", table=cust_pro)
g_first_name = Column(name="first_name", table=cust_pro)
g_father_name_1 = Column(name="father_name_1", table=cust_pro)
g_father_name_2 = Column(name="father_name_2", table=cust_pro)
g_gfather_name = Column(name="gfather_name", table=cust_pro)
g_last_name = Column(name="last_name", table=cust_pro)
g_gender = Column(name="gender", table=cust_pro)
g_mrtl_sta = Column(name="mrtl_sta", table=cust_pro)
g_monthly_sal = Column(name="monthly_sal", table=cust_pro)
g_ann_sal = Column(name="ann_sal", table=cust_pro)

# -------------------- ftca_tab Table Columns -------------------- #
h_cus_num = Column(name="led_code", table=ftca_tab)
h_iris_form_req = Column(name="iris_form_req", table=ftca_tab)

# -------------------- account.bra_code LPAD Function -------------------- #
b_bra_code_func = StrFunc(columns=(b_bra_code,))
b_bra_code_func.lpad_(4, '0')

# -------------------- account.cus_num LPAD Function -------------------- #
b_cus_num_func = StrFunc(columns=(b_cus_num,))
b_cus_num_func.lpad_(7, '0')

# -------------------- account.cur_code LPAD Function -------------------- #
b_cur_code_func = StrFunc(columns=(b_cur_code,))
b_cur_code_func.lpad_(3, '0')

# -------------------- account.led_code LPAD Function -------------------- #
b_led_code_func = StrFunc(columns=(b_led_code,))
b_led_code_func.lpad_(4, '0')

# -------------------- account.sub_acct_code LPAD Function -------------------- #
b_sub_acct_code_func = StrFunc(columns=(b_sub_acct_code,))
b_sub_acct_code_func.lpad_(3, '0')

# -------------------- Customer account_number Concat LPAD Functions Results -------------------- #
account_number_tuple = (str(b_bra_code_func), str(b_cus_num_func),
                        str(b_cur_code_func), str(b_led_code_func),
                        str(b_sub_acct_code_func))

account_number = StrFunc(columns=account_number_tuple)
account_number.concat_()

# -------------------- Customer name_ar Concat LPAD Functions Results -------------------- #
address_tbl_name_cols = (str(c_name_line1), f"' '", str(c_name_line2))
address_tbl_name = StrFunc(columns=address_tbl_name_cols)
address_tbl_name.concat_()

oth_add_tbl_name_cols = (str(d_name_line1), f"' '", str(d_name_line2))
oth_add_tbl_name = StrFunc(columns=oth_add_tbl_name_cols)
oth_add_tbl_name.concat_()

c_lan_ind_criterion1 = ValCriterion(left_attr=c_lan_ind, criteria=CriterionClause.Equal, values=[1])
c_lan_ind_criterion2 = ValCriterion(left_attr=c_lan_ind, criteria=CriterionClause.Equal, values=[2])

name_ar_case1 = CaseObjectContent(when=c_lan_ind_criterion1, then=str(oth_add_tbl_name))
name_ar_case2 = CaseObjectContent(when=c_lan_ind_criterion2, then=str(address_tbl_name))

name_ar_cases = DBFunc().case_(cases=[name_ar_case1, name_ar_case2])

# -------------------- Customer name_en Concat LPAD Functions Results -------------------- #

name_en_case1 = CaseObjectContent(when=c_lan_ind_criterion1, then=str(address_tbl_name))
name_en_case2 = CaseObjectContent(when=c_lan_ind_criterion2, then=str(oth_add_tbl_name))

name_en_cases = DBFunc().case_(cases=[name_en_case1, name_en_case2])


# -------------------- Customer middle_name Concat Function Results -------------------- #
cust_pro_tbl_mid_name_cols = (str(g_father_name_1), f"' '", str(g_father_name_2), f"' '", str(g_gfather_name))
cust_pro_tbl_mid_name = StrFunc(columns=cust_pro_tbl_mid_name_cols)
cust_pro_tbl_mid_name.concat_()

# -------------------- Gender Decode Function Results -------------------- #
cust_pro_gender_codes = ((0, f"'Female'"), (1, f"'Male'"), (2, f"'Not Defined'"), (f"'No Data'",))
cust_pro_gender = StrFunc(columns=(g_gender,))
cust_pro_gender.decode_(cust_pro_gender_codes)

# -------------------- Select Query Columns -------------------- #
columns_list = [a_bra_code, a_cus_num, g_first_name, g_last_name, a_nat_num, a_bir_date, a_birth_place, g_monthly_sal,
                g_ann_sal, c_add_line1, c_add_line2, d_add_line1, d_add_line2, c_street, c_po_box_num, c_postcode,
                c_tel_num, c_mob_num, c_email, c_fax_num, b_date_open, b_led_code, e_des_eng, f_des_eng]

# -------------------- Select Query Where Criteria (not including joins criteria) -------------------- #
customer_cus_num_criterion = ValCriterion(left_attr=a_cus_num, criteria=CriterionClause.Equal, values=[1])

account_sta_code_criterion = ValCriterion(left_attr=b_sta_code, criteria=CriterionClause.Equal, values=[1])
account_cur_code_criterion = ValCriterion(left_attr=b_cur_code, criteria=CriterionClause.Equal, values=[1])
account_led_code_criterion = ValCriterion(left_attr=b_led_code, criteria=CriterionClause.IsIn, values=[3000, 3001])

address_led_code_criterion = ValCriterion(left_attr=c_led_code, criteria=CriterionClause.Equal, values=[0])

oth_add_led_code_criterion = ValCriterion(left_attr=d_led_code, criteria=CriterionClause.Equal, values=[0])

print(customer_cus_num_criterion)
print(account_sta_code_criterion)
print(account_cur_code_criterion)
print(account_led_code_criterion)
print(address_led_code_criterion)
print(oth_add_led_code_criterion)

joined_tables = [customer, account, address, oth_add,
                 chrt_act, currency, cust_pro, ftca_tab]

criteria1 = AttrCriterion(left_attr=b_led_code, right_attr=e_led_code)
criteria2 = AttrCriterion(left_attr=b_cur_code, right_attr=f_cur_code)
criteria3 = AttrCriterion(left_attr=a_cus_num, right_attr=b_cus_num)
criteria4 = AttrCriterion(left_attr=a_cus_num, right_attr=c_cus_num)
criteria5 = AttrCriterion(left_attr=a_cus_num, right_attr=d_cus_num, join_expression='(+)')
criteria6 = AttrCriterion(left_attr=b_cus_num, right_attr=c_cus_num)
criteria7 = AttrCriterion(left_attr=a_bra_code, right_attr=b_bra_code)
criteria8 = AttrCriterion(left_attr=a_bra_code, right_attr=c_bra_code)
criteria9 = AttrCriterion(left_attr=a_bra_code, right_attr=d_bra_code, join_expression='(+)')
criteria10 = AttrCriterion(left_attr=b_bra_code, right_attr=c_bra_code)
criteria11 = AttrCriterion(left_attr=a_cus_num, right_attr=g_cus_num)
criteria12 = AttrCriterion(left_attr=a_bra_code, right_attr=g_bra_code)
criteria13 = AttrCriterion(left_attr=a_cus_num, right_attr=h_cus_num, join_expression='(+)')

where_criteria = [criteria1, criteria2, criteria3, criteria4, criteria5, criteria6, criteria7, criteria8, criteria9,
                  criteria10, criteria11, criteria12, criteria13]

join_clause = Join(cross_tables=joined_tables, where=where_criteria, join_clause=JoinClause.Cross)

print(str(join_clause))

# -------------------- Customer Title Sub Select -------------------- #

tab_id_criterion = ValCriterion(left_attr=tab_id, criteria=CriterionClause.Equal, values=[90])
tab_ent_criterion = ValCriterion(left_attr=tab_ent, criteria=CriterionClause.Equal, values=[a_tit_code])

print(tab_ent_criterion)
tab_id_criterion.and_ = [tab_ent_criterion]

statement_cus_title_select = Statement(criteria=[tab_id_criterion], clause=StatementClause.Where)
print(statement_cus_title_select)

cus_title_select = Select()
cus_title_select.tables_ = text_tab
cus_title_select.columns_ = [des_eng]
cus_title_select.where_ = statement_cus_title_select
cus_title_select.is_sub_query_ = True
cus_title_select.alias_ = 'CUS_TITLE'

print(cus_title_select)

# -------------------- ID_TYPE Sub Select -------------------- #

tab_id_criterion = ValCriterion(left_attr=tab_id, criteria=CriterionClause.Equal, values=[98])
tab_ent_criterion = ValCriterion(left_attr=tab_ent, criteria=CriterionClause.Equal, values=[a_id_type])

print(tab_ent_criterion)
tab_id_criterion.and_ = [tab_ent_criterion]

statement_cus_id_select = Statement(criteria=[tab_id_criterion], clause=StatementClause.Where)
print(statement_cus_id_select)

cus_id_type_select = Select()
cus_id_type_select.tables_ = text_tab
cus_id_type_select.columns_ = [des_eng]
cus_id_type_select.where_ = statement_cus_id_select
cus_id_type_select.is_sub_query_ = True
cus_id_type_select.alias_ = 'ID_TYPE'

print(cus_id_type_select)


# -------------------- MARITAL_STATUS Sub Select -------------------- #

tab_id_criterion = ValCriterion(left_attr=tab_id, criteria=CriterionClause.Equal, values=[647])
tab_ent_criterion = ValCriterion(left_attr=tab_ent, criteria=CriterionClause.Equal, values=[g_mrtl_sta])

print(tab_ent_criterion)
tab_id_criterion.and_ = [tab_ent_criterion]

statement_marital_status_select = Statement(criteria=[tab_id_criterion], clause=StatementClause.Where)
print(statement_marital_status_select)

marital_status_select = Select()
marital_status_select.tables_ = text_tab
marital_status_select.columns_ = [des_eng]
marital_status_select.where_ = statement_marital_status_select
marital_status_select.is_sub_query_ = True
marital_status_select.alias_ = 'MARITAL_STATUS'

print(marital_status_select)


# -------------------- CUS_CLASS Sub Select -------------------- #

tab_id_criterion = ValCriterion(left_attr=tab_id, criteria=CriterionClause.Equal, values=[55])
tab_ent_criterion = ValCriterion(left_attr=tab_ent, criteria=CriterionClause.Equal, values=[a_cus_class])

print(tab_ent_criterion)
tab_id_criterion.and_ = [tab_ent_criterion]

statement_cus_class_select = Statement(criteria=[tab_id_criterion], clause=StatementClause.Where)
print(statement_cus_class_select)

cus_class_select = Select()
cus_class_select.tables_ = text_tab
cus_class_select.columns_ = [des_eng]
cus_class_select.where_ = statement_cus_class_select
cus_class_select.is_sub_query_ = True
cus_class_select.alias_ = 'CUS_CLASS'

print(cus_class_select)


# -------------------- IRIS Sub Select -------------------- #

tab_id_criterion = ValCriterion(left_attr=tab_id, criteria=CriterionClause.Equal, values=[1670])
tab_ent_criterion = ValCriterion(left_attr=tab_ent, criteria=CriterionClause.Equal, values=[h_iris_form_req])

print(tab_ent_criterion)
tab_id_criterion.and_ = [tab_ent_criterion]

statement_cus_iris_select = Statement(criteria=[tab_id_criterion], clause=StatementClause.Where)
print(statement_cus_iris_select)

cus_iris_select = Select()
cus_iris_select.tables_ = text_tab
cus_iris_select.columns_ = [des_eng]
cus_iris_select.where_ = statement_cus_iris_select
cus_iris_select.is_sub_query_ = True
cus_iris_select.alias_ = 'IRIS'

print(cus_iris_select)

# -------------------- CITY Select -------------------- #

tab_id_criterion = ValCriterion(left_attr=tab_id, criteria=CriterionClause.Equal, values=[190])
tab_ent_criterion = ValCriterion(left_attr=tab_ent, criteria=CriterionClause.Equal, values=[c_city_loc_code])

print(tab_ent_criterion)
tab_id_criterion.and_ = [tab_ent_criterion]

statement_city_select = Statement(criteria=[tab_id_criterion], clause=StatementClause.Where)
print(statement_city_select)

city_select = Select()
city_select.tables_ = text_tab
city_select.columns_ = [des_eng]
city_select.where_ = statement_city_select
city_select.is_sub_query_ = True
city_select.alias_ = 'CITY'

print(city_select)


# -------------------- COU_OF_RES Select -------------------- #

tab_id_criterion = ValCriterion(left_attr=tab_id, criteria=CriterionClause.Equal, values=[10])
tab_ent_criterion = ValCriterion(left_attr=tab_ent, criteria=CriterionClause.Equal, values=[a_cou_of_resi])

print(tab_ent_criterion)
tab_id_criterion.and_ = [tab_ent_criterion]

statement_cou_of_res_select = Statement(criteria=[tab_id_criterion], clause=StatementClause.Where)
print(statement_cou_of_res_select)

cou_of_res_select = Select()
cou_of_res_select.tables_ = text_tab
cou_of_res_select.columns_ = [des_eng]
cou_of_res_select.where_ = statement_cou_of_res_select
cou_of_res_select.is_sub_query_ = True
cou_of_res_select.alias_ = 'COU_OF_RES'

print(cou_of_res_select)


# -------------------- ACCT_STATUS Select -------------------- #

tab_id_criterion = ValCriterion(left_attr=tab_id, criteria=CriterionClause.Equal, values=[5])
tab_ent_criterion = ValCriterion(left_attr=tab_ent, criteria=CriterionClause.Equal, values=[a_cou_of_resi])

print(tab_ent_criterion)
tab_id_criterion.and_ = [tab_ent_criterion]

statement_acc_status_select = Statement(criteria=[tab_id_criterion], clause=StatementClause.Where)
print(statement_acc_status_select)

acc_status_select = Select()
acc_status_select.tables_ = text_tab
acc_status_select.columns_ = [des_eng]
acc_status_select.where_ = statement_acc_status_select
acc_status_select.is_sub_query_ = True
acc_status_select.alias_ = 'ACCT_STATUS'

print(acc_status_select)


