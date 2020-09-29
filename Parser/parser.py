import json

TAGS = ['select', 'from', 'where', 'group', 'having']


class Parse:

    def __init__(self, query, schema):
        self.groupByColumnIndex = []
        self.selectColumnIndex = []
        self.parsedQuery = {}
        self.query = query
        self.selectColumns = []
        self.selectFunc = []
        self.fromTable = []
        self.groupByColumns = []
        self.havingCondition = []
        self.schema = schema

    def buildQueryElements(self):
        query = self.query
        query = query.strip()
        query = query.split()
        query.remove('by')
        queryElements = []
        elements = ''
        for item in query[1:]:
            if item.lower() in TAGS:
                queryElements.append(elements)
                elements = ''
            else:
                elements += item
        queryElements.append(elements)
        return queryElements

    def parseQuery(self):
        queryElements = self.buildQueryElements()
        self.parseSelectElements(queryElements[0])
        self.parseFromTable(queryElements[1])
        self.parseWhereCondition(queryElements[2])
        self.parseGroupByElements(queryElements[3])
        self.parseHavingCondition(queryElements[4])

    def parseSelectElements(self, elements):
        if ',' in elements:
            elements = elements.split(',')
            for element in elements:
                if '(' not in element:
                    self.selectColumns.append(element.strip())
                else:
                    temp = [element.split('(')[0].lower().strip(), element.split('(')[1][:-1].strip()]
                    self.selectFunc.append(temp)
        else:
            element = elements
            if '(' not in element:
                self.selectColumns.append(element.strip())
            else:
                self.selectFunc.append(element.split('(')[0].lower().strip())
                self.selectFunc.append(element.split('(')[1][:-1].strip())

        self.parsedQuery['selectColumns'] = self.selectColumns
        self.parsedQuery['selectFunc'] = self.selectFunc

    def parseGroupByElements(self, elements):
        if ',' in elements:
            elements = elements.split(',')
            self.groupByColumns = [element.strip() for element in elements]
        else:
            self.groupByColumns.append(elements.strip())

        self.parsedQuery['groupByColumns'] = self.groupByColumns

    def parseHavingCondition(self, elements):
        operators = ['<=', '>=', '!=', '=', '<', '>']
        for operator in operators:
            if operator in elements:
                threshold = elements.split(operator)[1].strip().lower()
                self.havingCondition = [threshold, operator]
                break
        self.parsedQuery['havingCondition'] = self.havingCondition

    def parseFromTable(self, elements):
        if ',' in elements:
            elements = elements.split(',')
            self.fromTable = [element.strip() for element in elements]
        else:
            self.fromTable.append(elements.strip())
        self.parsedQuery['fromTable'] = self.fromTable

    def parseWhereCondition(self, elements):
        elements = elements.strip().split('=')
        whereValue = elements[1].strip()
        whereColumn = elements[0].strip()
        self.parsedQuery['whereValue'] = whereValue.strip('"')
        self.parsedQuery['whereColumn'] = whereColumn

    def getParsedQuery(self):
        self.parseQuery()
        # self.assignQueryElements()
        return self.parsedQuery

    def assignQueryElements(self):

        fromTable = self.parsedQuery['fromTable'][0]
        columns = list(self.schema[fromTable])
        for column in self.parsedQuery['selectColumns']:
            self.selectColumnIndex.append(columns.index(column))
        selectFuncColumnIndex = columns.index(self.parsedQuery['selectFunc'][0][1])
        aggregationFunction = self.parsedQuery['selectFunc'][0][0]
        for column in self.parsedQuery['groupByColumns']:
            self.groupByColumnIndex.append(columns.index(column))
        havingOperator = self.parsedQuery['havingCondition'][1]
        havingThreshold = self.parsedQuery['havingCondition'][0]
        whereValue = self.parsedQuery['whereValue']
        whereColumnIndex = columns.index(self.parsedQuery['whereColumn'])

        elements = {
            'selectColumnIndex': self.selectColumnIndex,
            'aggregationFunction': aggregationFunction,
            'selectFuncColumnIndex': selectFuncColumnIndex,
            'fromTable': fromTable,
            'whereColumnIndex': whereColumnIndex,
            'whereValue': whereValue,
            'groupByColumnIndex': self.groupByColumnIndex,
            'havingOperator': havingOperator,
            'havingThreshold': havingThreshold
        }

        with open('Dependencies/elements.json', 'w') as target:
            json.dump(elements, target)


# if __name__ == "__main__":
#     query = 'Select col1, col2, col3, count(col4) from table1, table2 where col2 =
#     "value" group by col6, col7 having col1 >= 3'
#     parser = Parse(query, {'hello':'test'})
#     q = parser.getParsedQuery()
#     print(q)
    # for k, v in q.items():
    #     print(k, v)
