import os


class MRSession:

    def __init__(self, config, fromTable):
        self.config = config
        self.fromTable = fromTable

    def executeQuery(self):
        initiate_command = self.config['initiate_command']
        os.system(initiate_command)
        cmd = 'hadoop jar {hadoop_streaming_jar} -file {element_path} -file {mapper_path} ' \
              '-mapper mapper.py -file {reducer_path} -reducer reducer.py -input {input_dir}/{table}.csv -output ' \
              '{output_dir}'.format(
            hadoop_streaming_jar=self.config['hadoop_streaming_jar'],
            mapper_path=self.config['mapper_path'],
            reducer_path=self.config['reducer_path'],
            input_dir=self.config['input_dir'],
            table=self.fromTable,
            output_dir=self.config['output_dir'],
            element_path=self.config['element_path'])
        os.system(cmd)
