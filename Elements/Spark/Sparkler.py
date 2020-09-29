import time
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class Sparkler:

    def __init__(self, config):
        self.sparkResult = None
        self.actions = ['groupby', 'agg', 'where']
        self.timeTaken = None
        self.config = config
        self.fieldType = {
            'IntegerType': IntegerType(),
            'StringType': StringType()
        }

    def initiateSparkler(self):
        spark = SparkSession.builder.master('local').appName('sparkler').getOrCreate()
        return spark

    def loadData(self, structureField, fromTable):
        spark = self.initiateSparkler()
        structureField = structureField[fromTable]
        schemaList = []
        for field, fieldType in structureField.items():
            schemaList.append(StructField(field, self.fieldType[fieldType], True))

        schema = StructType(schemaList)
        table = spark.read.csv(
            'hdfs://localhost:9000{input_dir}/{table}.csv'.format(input_dir=self.config['input_dir'],
                                                                  table=fromTable), schema=schema)
        return table

    def executeQuery(self, parsedQuery, structureField, fromTable):
        start = time.time()
        table = self.loadData(structureField, fromTable)
        groupByResult = table.groupBy(parsedQuery['groupByColumns'])
        aggResult = groupByResult.agg({str(parsedQuery['selectFunc'][0][1]): str(parsedQuery['selectFunc'][0][0])})
        havingCondition = '{selectFunc}({column}){havingOperator}{havingThreshold}'.format(
            selectFunc=parsedQuery['selectFunc'][0][0],
            column=parsedQuery['selectFunc'][0][1],
            havingOperator=parsedQuery['havingCondition'][1],
            havingThreshold=parsedQuery['havingCondition'][0]
        )
        havingResult = aggResult.where(havingCondition)
        self.timeTaken = time.time()-start
        self.sparkResult = {
            'result': havingResult,
            'timeTaken': self.timeTaken,
            'transformationActions': self.actions
        }
        return self.sparkResult


