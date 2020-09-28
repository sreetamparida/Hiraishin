#!/usr/bin/python3
"""reducer.py"""
import operator
import yaml
import sys


class Reducer:

    def __init__(self, element):
        self.elements = element
        self.havingThreshold = self.elements['havingThreshold']
        self.havingOperator = self.elements['havingOperator']
        self.aggregationFunction = self.elements['aggregationFunction']
        self.data = {}
        self.func = {
            'max': max,
            'sum': sum,
            'min': min,
            'count': len
        }
        self.operate = {
            '=': operator.eq,
            '<': operator.lt,
            '>': operator.gt,
            '<=': operator.le,
            '>=': operator.ge,
            '!=': operator.ne
        }

    def performOperation(self, havingOperator, havingThreshold, aggregationFunction, data):
        for column in data:
            if aggregationFunction == 'count':
                aggregateValue = self.func[aggregationFunction](data[column])
            else:
                values = [int(value) for value in data[column]]
                aggregateValue = self.func[aggregationFunction](values)
            if self.operate[havingOperator](aggregateValue, int(havingThreshold)):
                print(str(column) + ', ' + str(aggregateValue))

    def reduce(self):
        for row in sys.stdin:
            row = row.strip().split('|')
            if row[0] not in self.data:
                self.data[row[0]] = [row[1]]
            else:
                self.data[row[0]].append(row[1])
        self.performOperation(self.havingOperator, self.havingThreshold, self.aggregationFunction, self.data)


if __name__ == '__main__':
    with open('Dependencies/elements.yaml', 'r') as file:
        elements = yaml.load(file, Loader=yaml.FullLoader)

    reducer = Reducer(elements)
    reducer.reduce()
