from __future__ import annotations
# from typing import List
from dpsql.table.model.criterion_obj import Criterion


class CaseObjectContent(object):
    def __init__(self, **kwargs):
        self._when_criteria: Criterion = kwargs.get('when', None)
        self._then: str = kwargs.get('then', '')

    @property
    def when_criteria_(self):
        return self._when_criteria

    @property
    def then_(self):
        return self._then

    @when_criteria_.setter
    def when_criteria_(self, param):
        self._when_criteria = param

    @then_.setter
    def then_(self, param):
        self._then = param

#
# class Case(object):
#     def __init__(self, **kwargs):
#         self._cases: List[CaseObjectContent] = kwargs.get('cases', '')
#         self._else: str = kwargs.get('else', '')
#
#     def __str__(self):
#         return self._case_builder()
#
#     def __repr__(self):
#         return self._case_builder()
#
#     @property
#     def cases_(self):
#         return self._cases
#
#     @property
#     def else_(self):
#         return self._else
#
#     @cases_.setter
#     def cases_(self, param):
#         self._cases = param
#
#     @else_.setter
#     def else_(self, param):
#         self._else = param
#
#     def _case_builder(self):
#         opt_res = 'CASE '
#         res = ''
#         for case in self.cases_:
#             res += f"WHEN {str(case.when_criteria_)} THEN {case.then_} "
#
#         if self.else_ != '':
#             res += f"ELSE {self.else_}"
#
#         opt_res += f"{res} END"
#         return opt_res
