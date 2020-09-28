import yaml
import os


class MRSession:

    def __init__(self, config):
        self.config = config

    def executeQuery(self):

        cmd = 'python3 Elements/MapRed/mapper.py'
        os.system(cmd)
