import os


class MRSession:

    def __init__(self, config, fromTable):
        self.config = config
        self.fromTable = fromTable

    def executeQuery(self):
        initiate_command = self.config['initiate_command']
        os.system(initiate_command)
        cmd = 'hadoop jar {hadoop_streaming_jar} -mapper "python3 {mapper_path}" -reducer "python3 ' \
              '{reducer_path}" -input /{input_dir}/{table}.csv -output {output_dir}'.format(
            hadoop_streaming_jar=self.config['hadoop_streaming_jar'],
            mapper_path=self.config['mapper_path'],
            reducer_path=self.config['reducer_path'],
            input_dir=self.config['input_dir'],
            table=self.fromTable,
            output_dir=self.config['output_dir'])
        os.system(cmd)
