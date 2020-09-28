from Parser.parser import Parse
from Elements.MapRed.MRSession import MRSession
from Elements.MapRed.MRResult import MRResult
import yaml


class Driver:

    def __init__(self, query):
        self.query = query
        self.parsedQuery = {}
        self.schema = {}
        self.config = {}
        self.MAP_RED_RESULT = {}

    def getDependencies(self):
        with open('Dependencies/schema.yaml', 'r') as file:
            self.schema = yaml.load(file, Loader=yaml.FullLoader)
        with open('Dependencies/config.yaml', 'r') as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)
        self.parsedQuery = Parse(self.query, self.schema).getParsedQuery()

    def run(self):
        self.getDependencies()
        mrSession = MRSession(self.config, self.parsedQuery['fromTable'][0])
        timeTaken = mrSession.executeQuery()
        columns = [','.join(self.parsedQuery['selectColumns']), ' '.join(self.parsedQuery['selectFunc'][0])]
        mrResult = MRResult(self.config, columns, timeTaken)
        self.MAP_RED_RESULT = mrResult.getResult()
        print(self.MAP_RED_RESULT)


# if __name__ == '__main__':
#     driver = Driver()
#     driver.getDependencies()
#     MRSession = MRSession(driver.getParsedQuery(), driver.schema, driver.config)
#     MRSession.assignQueryElements()
#     print()
