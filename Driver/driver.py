from Parser.parser import Parse
from Elements.MapRed.MRSession import MRSession
from Elements.MapRed.MRResult import MRResult
from Elements.Spark.Sparkler import Sparkler
import yaml


class Driver:

    def __init__(self, query):
        self.query = query
        self.parsedQuery = {}
        self.schema = {}
        self.config = {}
        self.structureField = {}
        self.MAP_RED_RESULT = {}
        self.SPARK_RESULT = {}
        self.columns = []
        self.fromTable = ''

    def getDependencies(self):
        with open('Dependencies/schema.yaml', 'r') as file:
            self.schema = yaml.load(file, Loader=yaml.FullLoader)
        with open('Dependencies/config.yaml', 'r') as file:
            self.config = yaml.load(file, Loader=yaml.FullLoader)
        with open('Dependencies/structure_field.yaml', 'r') as file:
            self.structureField = yaml.load(file, Loader=yaml.FullLoader)
        self.parsedQuery = Parse(self.query, self.schema).getParsedQuery()
        self.columns = [','.join(self.parsedQuery['selectColumns']), ' '.join(self.parsedQuery['selectFunc'][0])]
        self.fromTable = self.parsedQuery['fromTable'][0]

    def runMapRed(self):
        mrSession = MRSession(self.config, self.fromTable)
        mrResult = MRResult(self.config, self.columns, mrSession.executeQuery())
        self.MAP_RED_RESULT = mrResult.getResult()

    def runSparkler(self):
        sparkler = Sparkler(self.config)
        self.SPARK_RESULT = sparkler.executeQuery(self.parsedQuery, self.structureField, self.fromTable)

    def run(self):
        self.getDependencies()
        self.runMapRed()
        self.runSparkler()
        print(self.MAP_RED_RESULT)
        print(self.SPARK_RESULT)


# if __name__ == '__main__':
#     driver = Driver()
#     driver.getDependencies()
#     MRSession = MRSession(driver.getParsedQuery(), driver.schema, driver.config)
#     MRSession.assignQueryElements()
#     print()
