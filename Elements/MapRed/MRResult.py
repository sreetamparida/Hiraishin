import os


class MRResult:

    def __init__(self, config, columns, timeTaken):
        self.mrResult = {}
        self.queryResult = []
        self.config = config
        self.columns = columns
        self.timeTaken = timeTaken

    def getResult(self):
        cmd = 'hdfs dfs -get {outputDir} {localOutputDir}'.format(outputDir=self.config['output_dir'],
                                                                  localOutputDir=self.config['local_output_dir'])
        os.system(cmd)

        localFilePath = '{localOutputDir}/part-00000'.format(localOutputDir=self.config['local_output_dir'])
        with open(localFilePath, 'r') as f:
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

        cleanDirectory = self.config['clean_directory']
        cleanHDFS = self.config['clean_hdfs']
        os.system(cleanDirectory)
        os.system(cleanHDFS)

        return self.mrResult




