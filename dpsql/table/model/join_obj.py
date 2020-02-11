# from enum import Enum
# from typing import List
# import logging
# from dpsql.table.model.criterion_obj import AttrCriterion, ValCriterion
# from dpsql.table.model.db_objects import QueryTable, Column
#
#
# class JoinClause(Enum):
#     Inner = 1
#     LeftOuter = 2
#     RightOuter = 3
#     Cross = 4
#     Natural = 5
#     NaturalRight = 6
#     NaturalLeft = 7
#
#
# class Join(object):
#     def __init__(self, **kwargs):
#         self._left_table: QueryTable = kwargs.get('left_table', None)
#         self._right_table: QueryTable = kwargs.get('right_table', None)
#         self._type: JoinClause = kwargs.get('join_clause', None)
#         self._cross_join_tables: List[QueryTable] = kwargs.get('cross_tables', None)
#         self._on_attribs_criteria: List[AttrCriterion] = kwargs.get('on', None)
#         self._where_attribs_criteria: List[AttrCriterion] = kwargs.get('where', None)
#         self._join_str: str = ''
#
#     def __str__(self):
#         self._switch_join()
#         return self.join_str_
#
#     def __repr__(self):
#         self._switch_join()
#         return self.join_str_
#
#     @property
#     def left_table_(self):
#         return self._left_table
#
#     @property
#     def right_table_(self):
#         return self._right_table
#
#     @property
#     def type_(self):
#         return self._type
#
#     @property
#     def join_str_(self):
#         return self._join_str
#
#     @property
#     def cross_join_tables_(self):
#         return self._cross_join_tables
#
#     @property
#     def on_attribs_criteria_(self):
#         return self._on_attribs_criteria
#
#     @property
#     def where_attribs_criteria_(self):
#         return self._where_attribs_criteria
#
#     @left_table_.setter
#     def left_table_(self, param):
#         self._left_table = param
#
#     @right_table_.setter
#     def right_table_(self, param):
#         self._right_table = param
#
#     @type_.setter
#     def type_(self, param):
#         self._type = param
#
#     @join_str_.setter
#     def join_str_(self, param):
#         self._join_str = param
#
#     @cross_join_tables_.setter
#     def cross_join_tables_(self, param):
#         self._cross_join_tables = param
#
#     @on_attribs_criteria_.setter
#     def on_attribs_criteria_(self, param):
#         self._on_attribs_criteria = param
#
#     @where_attribs_criteria_.setter
#     def where_attribs_criteria_(self, param):
#         self._where_attribs_criteria = param
#
#     def join_to_string(self):
#         self._switch_join()
#         return self.join_str_
#
#     def _switch_join(self):
#         res_dict = {
#             'Inner': self._inner_,
#             'LeftOuter': self._left_outer_,
#             'RightOuter': self._right_outer_,
#             'Cross': self._cross_,
#             'Natural': self._natural_,
#             'NaturalLeft': self._natural_left_,
#             'NaturalRight': self._natural_right_,
#         }
#         return res_dict.get(self.type_.name, '')()
#
#     def _inner_(self):
#         logger = logging.getLogger(__name__)
#         res = ''
#
#         try:
#             left_t = self.left_table_.table_name_getter()
#             right_t = self.right_table_.table_name_getter()
#             res = f"{left_t} inner join {right_t} on ("
#             res += f"{' and '.join(str(crit) for crit in self.on_attribs_criteria_)}) "
#             self.join_str_ = res
#
#         except TypeError as error:
#             logger.exception('The Join does not contain On attributes', exc_info=True)
#
#     def _left_outer_(self):
#         logger = logging.getLogger(__name__)
#         res = ''
#
#         try:
#             left_t = self.left_table_.table_name_getter()
#             right_t = self.right_table_.table_name_getter()
#             res = f"{left_t} left outer join {right_t} on ("
#             res += f"{' and '.join(str(crit) for crit in self.on_attribs_criteria_)})"
#             self.join_str_ = res
#
#         except TypeError as error:
#             logger.exception('The Join does not contain On attributes', exc_info=True)
#
#     def _right_outer_(self):
#         logger = logging.getLogger(__name__)
#         res = ''
#
#         try:
#             left_t = self.left_table_.table_name_getter()
#             right_t = self.right_table_.table_name_getter()
#             res = f"{left_t} right outer join {right_t} on ("
#             res += f"{' and '.join(str(crit) for crit in self.on_attribs_criteria_)})"
#             self.join_str_ = res
#
#         except TypeError as error:
#             logger.exception('The Join does not contain On attributes', exc_info=True)
#
#     def _cross_(self):
#         logger = logging.getLogger(__name__)
#         res = ''
#
#         try:
#             res = f"{', '.join(table_in.table_name_getter() for table_in in self.cross_join_tables_)}"
#             if len(self.cross_join_tables_) == 2:
#                 if self.on_attribs_criteria_:
#                     res += f" on ({' and '.join(str(crit) for crit in self.on_attribs_criteria_)})"
#                 # elif self.where_attribs_criteria_:
#                 #     res += f" Where {' and '.join(str(crit) for crit in self.where_attribs_criteria_)}"
#             self.join_str_ = res
#
#         except TypeError as error:
#             logger.exception('The Join does not contain On or Where attributes', exc_info=True)
#
#     def _natural_(self):
#         logger = logging.getLogger(__name__)
#         res = ''
#
#         try:
#             left_t = self.left_table_.table_name_getter()
#             right_t = self.right_table_.table_name_getter()
#             res = f"{left_t} natural join {right_t} on ("
#             res += f"{' and '.join(str(crit) for crit in self.on_attribs_criteria_)})"
#             self.join_str_ = res
#         except TypeError as error:
#             logger.exception('The Join does not contain On attributes', exc_info=True)
#
#     def _natural_left_(self):
#         logger = logging.getLogger(__name__)
#         res = ''
#
#         try:
#             left_t = self.left_table_.table_name_getter()
#             right_t = self.right_table_.table_name_getter()
#             res = f"{left_t} natural left join {right_t} on ("
#             res += f"{' and '.join(str(crit) for crit in self.on_attribs_criteria_)})"
#             self.join_str_ = res
#
#         except TypeError as error:
#             logger.exception('The Join does not contain On attributes', exc_info=True)
#
#     def _natural_right_(self):
#         logger = logging.getLogger(__name__)
#         res = ''
#
#         try:
#             left_t = self.left_table_.table_name_getter()
#             right_t = self.right_table_.table_name_getter()
#             res = f"{left_t} natural right join {right_t} on ("
#             res += f"{' and '.join(str(crit) for crit in self.on_attribs_criteria_)})"
#             self.join_str_ = res
#
#         except TypeError as error:
#             logger.exception('The Join does not contain On attributes', exc_info=True)
#
#
#
