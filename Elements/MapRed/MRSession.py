import yaml
import os


class MRSession:

    def __init__(self, parsedQuery, schema, config):
        self.query = parsedQuery
        self.schema = schema
        self.config = config
        self.elements = {}
        self.fromTable = ''
        self.selectColumnIndex = []
        self.selectFuncColumnIndex = []
        self.groupByColumnIndex = []
        self.havingThreshold = ''
        self.havingOperator = ''
        self.aggregationFunction = ''

    def assignQueryElements(self):

        self.fromTable = self.query['fromTable'][0]
        columns = list(self.schema[self.fromTable].keys())
        for column in self.query['selectColumns']:
            self.selectColumnIndex.append(columns.index(column))
        self.selectFuncColumnIndex = columns.index(self.query['selectFunc'][0][1])
        self.aggregationFunction = self.query['selectFunc'][0][0]
        for column in self.query['groupByColumns']:
            self.groupByColumnIndex.append(columns.index(column))
        self.havingOperator = self.query['havingCondition'][1]
        self.havingThreshold = self.query['havingCondition'][0]

        self.elements = {
            'selectColumnIndex': self.selectColumnIndex,
            'aggregationFunction': self.aggregationFunction,
            'fromTable': self.fromTable,
            'groupByColumnIndex': self.groupByColumnIndex,
            'havingOperator': self.havingOperator,
            'havingThreshold': self.havingThreshold
        }

    def executeQuery(self):
        self.assignQueryElements()
        with open('Dependencies/elements.yaml', 'w') as target:
            yaml.dump(self.elements, target)
        cmd = 'python3 Elements/MapRed/mapper.py'
        os.system(cmd)
