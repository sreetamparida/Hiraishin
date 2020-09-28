import os


class MRResult:

    def __init__(self, config, columns, timeTaken):
        self.mrResult = {}
        self.queryResult = []
        self.config = config
        self.columns = columns
        self.timeTaken = timeTaken

    def getResult(self):
        cmd = 'hdfs dfs -get {outputDir} ~/map_red_output'.format(outputDir=self.config['output_dir'])
        os.system(cmd)

        with open('~/map_red_output/part-00000', 'r') as f:
            results = f.readlines()

        for result in results:
            result = result.replace('\n', '').strip().split('\t')
            tempDict = {
                self.columns[0]: result[0],
                self.columns[1]: result[1]
            }
            self.queryResult.append(tempDict)

        self.mrResult['result'] = self.queryResult
        self.mrResult['timeTaken'] = self.timeTaken

        return self.mrResult




