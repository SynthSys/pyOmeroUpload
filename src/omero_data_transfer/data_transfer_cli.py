#!/usr/bin/env python
# -*- coding: utf-8 -*-

# override installed pyOmeroUpload package
import sys
#sys.path.insert(1, '/home/jovyan/work/pyOmeroUpload/src')
print sys.path

import argparse
from data_transfer_manager import DataTransferManager

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")

# alter below CLIENT_TYPE var to switch between executable jars
#CLIENT_TYPE = "importer"
CLIENT_TYPE = "cli"

CLIENT_JAR_NAME = ".".join(["-".join(["omero", CLIENT_TYPE]), "jar"])
CLIENT_JAR_PATH = os.path.join(PROJECT_DIR, 'resources', CLIENT_JAR_NAME)
CONFIG_FILE = os.path.join(PROJECT_DIR, 'config.yml')
CONFIG = {}

# Instantiate the parser
parser = argparse.ArgumentParser(description='PyOmeroUpload Data Transfer Application')

# mandatory args
parser.add_argument('-d', '--data-path', dest='data_path',
    type=str, required=True, metavar='data-path',
    help="specifies the system file path to the data directory for uploading")

parser.add_argument('-n', '--dataset-name', dest='dataset_name',
    type=str, required=True, metavar='dataset-name',
    help="specifies the name of the destination dataset")

# optional args
parser.add_argument('-c', '--hypercube', dest='hypercube',
    type=bool, required=False, metavar='hypercube',
    help="commands the uploader to generate hypercube images")

parser.add_argument('-m', '--module-path', dest='module_path',
    type=str, required=False, metavar='module-path',
    help="specifies the system file path to the directory containing custom classes")

parser.add_argument('-p', '--parser-class', dest='parser_class',
    type=str, required=False, metavar='parser-class',
    help="specifies the name of the custom metadata parser class")

parser.add_argument('-i', '--image-processor-class', dest='image_processor_class',
    type=str, required=False, metavar='image-processor-class',
    help="specifies the name of the custom image processor class")

args = parser.parse_args()

if args.data_path is not None and args.dataset_name is not None:
    # validate args
    if args.data_path.strip() is "":
        print "Data path is empty"
        quit()

    if args.dataset_name.strip() is "":
        print "Dataset name is empty"
        quit()

    conn_settings = CONFIG['test_settings']['omero_conn']
    broker = OMERODataBroker(username=conn_settings['username'],
                             password=conn_settings['password'],
                             host=conn_settings['server'],
                             port=conn_settings['port'],
                             image_processor=image_processor_impl())
    print "hello"
    broker.open_omero_session()

    # dir_path = os.path.join("","/var","data_dir")
    # dir_path = os.path.join(PROJECT_DIR,"..","Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")

    data_transfer_manager = DataTransferManager()
    data_transfer_manager.upload_data_dir(broker, data_path, import_images=False)
    # upload_metadata(broker, dir_path)
    print "hello2"
    broker.close_omero_session()

if args.hypercube:
    print args.hypercube

if args.module_path:
    print args.module_path

if args.parser_class:
    print args.parser_class

if args.image_processor_class:
    print args.image_processor_class
