TAGS = ['select', 'from', 'group', 'having']


class Parse:

    def __init__(self, query):
        self.parsedQuery = {}
        self.query = query
        self.selectColumns = []
        self.selectFunc = []
        self.fromTable = []
        self.groupByColumns = []
        self.havingCondition = []

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
        self.parseGroupByElements(queryElements[2])
        self.parseHavingCondition(queryElements[3])

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

    def getParsedQuery(self):
        self.parseQuery()
        return self.parsedQuery


# if __name__ == "__main__":
#     query = 'Select col1, col2, col3, count(col4) from table1, table2 group by col6, col7 having col1 >= 3'
#     parser = Parse(query)
#     q = parser.getParsedQuery()
#     print(q)
#     for k, v in q.items():
#         print(k, v)
