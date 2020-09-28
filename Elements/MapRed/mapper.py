#!/usr/bin/python3
"""mapper.py"""
import yaml
import sys


class Mapper:

    def __init__(self, element):
        self.selectColumnIndex = element['selectColumnIndex']
        self.selectFuncColumnIndex = element['selectFuncColumnIndex']
        self.groupByColumnIndex = element['groupByColumnIndex']

    def execute(self):
        for row in sys.stdin:
            row = row.strip().split(',')
            if len(self.selectColumnIndex) == len(self.groupByColumnIndex):
                groupByColumns = [row[index] for index in self.groupByColumnIndex]
                aggregationColumn = row[self.selectFuncColumnIndex]
                groupByColumns = ','.join(groupByColumns)
                print(groupByColumns + '|' + str(aggregationColumn))


if __name__ == '__main__':
    with open('Dependencies/elements.yaml', 'r') as file:
        elements = yaml.load(file, Loader=yaml.FullLoader)
    print(elements)
    mapper = Mapper(elements)
    mapper.execute()
