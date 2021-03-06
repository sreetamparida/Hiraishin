import time
import json
import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


class Sparkler:

    def __init__(self, config):
        self.sparkResult = None
        self.actions = ['where', 'groupby', 'agg', 'where']
        self.timeTaken = None
        self.config = config
        self.fieldType = {
            'IntegerType': IntegerType(),
            'StringType': StringType(),
            'FloatType': FloatType()
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
        if parsedQuery['whereOperator'] == '=':
            whereOperator = '=='
        else:
            whereOperator = parsedQuery['whereOperator']
        if structureField[fromTable][parsedQuery['whereColumn']] == 'StringType':
            whereCondition = '{whereColumn}{whereOperator}"{whereValue}"'.format(
                whereColumn=parsedQuery['whereColumn'],
                whereValue=parsedQuery['whereValue'],
                whereOperator=whereOperator
            )
        else:
            whereCondition = '{whereColumn}{whereOperator}{whereValue}'.format(
                whereColumn=parsedQuery['whereColumn'],
                whereValue=parsedQuery['whereValue'],
                whereOperator=whereOperator
            )

        whereResult = table.where(whereCondition)
        groupByResult = whereResult.groupBy(parsedQuery['groupByColumns'])
        aggResult = groupByResult.agg({str(parsedQuery['selectFunc'][0][1]): str(parsedQuery['selectFunc'][0][0])})
        havingCondition = '{selectFunc}({column}){havingOperator}{havingThreshold}'.format(
            selectFunc=parsedQuery['selectFunc'][0][0],
            column=parsedQuery['selectFunc'][0][1],
            havingOperator=parsedQuery['havingCondition'][1],
            havingThreshold=parsedQuery['havingCondition'][0]
        )
        havingResult = aggResult.where(havingCondition)
        havingResult = havingResult.toJSON().map(lambda j: json.loads(j)).collect()
        self.timeTaken = time.time() - start
        self.sparkResult = {
            'result': havingResult,
            'timeTaken': self.timeTaken,
            'transformationActions': self.actions
        }
        return self.sparkResult
