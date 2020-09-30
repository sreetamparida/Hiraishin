#!/usr/bin/python3
"""mapper.py"""
import json
import sys
import operator


class Mapper:

    def __init__(self, element):
        self.selectColumnIndex = element['selectColumnIndex']
        self.selectFuncColumnIndex = element['selectFuncColumnIndex']
        self.groupByColumnIndex = element['groupByColumnIndex']
        self.whereColumnIndex = element['whereColumnIndex']
        self.whereValue = element['whereValue']
        self.whereOperator = element['whereOperator']
        self.operate = {
            '=': operator.eq,
            '<': operator.lt,
            '>': operator.gt,
            '<=': operator.le,
            '>=': operator.ge,
            '!=': operator.ne
        }

    def execute(self):
        for row in sys.stdin:
            row = row.strip().split(',')
            if self.operate[self.whereOperator](row[self.whereColumnIndex], self.whereValue):
                groupByColumns = [row[index] for index in self.groupByColumnIndex]
                aggregationColumn = row[self.selectFuncColumnIndex]
                groupByColumns = ','.join(groupByColumns)
                print(groupByColumns + '\t' + str(aggregationColumn))


if __name__ == '__main__':
    with open('elements.json', 'r') as file:
        elements = json.load(file)
    mapper = Mapper(elements)
    mapper.execute()
