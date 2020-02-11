
import argparse

# Instantiate the parser
parser = argparse.ArgumentParser(description='PyOmeroUpload Data Transfer Application')

# mandatory args
parser.add_argument('-d', '--data-path', dest='data_path',
    type=string, required=True, action='store_true', metavar='data-path',
    help="specifies the system file path to the data directory for uploading")

parser.add_argument('-n', '--dataset-name', dest='dataset_name',
    type=string, required=True, action='store_true', metavar='dataset-name',
    help="specifies the name of the destination dataset")

# optional args
parser.add_argument('-h', '--hypercube', dest='format',
    type=bool, required=False, action='store_true',  metavar='hypercube',
    help="commands the uploader to generate hypercube images")

parser.add_argument('-m', '--module-path', dest='format',
    type=string, required=False, action='store_true', metavar='module-path',
    help="specifies the system file path to the directory containing custom classes")

parser.add_argument('-p', '--parser-class', dest='format',
    type=string, required=False, action='store_true', metavar='arser-class',
    help="specifies the name of the custom metadata parser class")

parser.add_argument('-i', '--image-processor-class', dest='format',
    type=string, required=False, action='store_true', metavar='image-processor-class',
    help="specifies the name of the custom image processor class")