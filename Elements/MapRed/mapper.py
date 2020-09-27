#!/usr/bin/python3
import yaml


class Mapper:

    def __init__(self):
        self.q = ''


if __name__ == '__main__':
    with open('Dependencies/elements.yaml', 'r') as file:
        elements = yaml.load(file, Loader=yaml.FullLoader)
    print(elements)
