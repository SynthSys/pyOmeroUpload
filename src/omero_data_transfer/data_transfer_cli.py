#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

# override installed pyOmeroUpload package
import sys
sys.path.insert(1, '/home/jovyan/work/pyOmeroUpload/src')
print sys.path

import os
import argparse
import yaml
from omero_data_transfer.data_transfer_manager import DataTransferManager
from omero_data_transfer.omero_data_broker import OMERODataBroker

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

parser.add_argument('-p', '--custom-metadata-parser', dest='custom_metadata_parser',
    type=bool, required=False, metavar='custom-metadata-parser',
    help="commands the uploader to use a custom parser class located in the module path")

parser.add_argument('-i', '--custom-image-processor', dest='custom_image_processor',
    type=bool, required=False, metavar='custom-image-processor',
    help="commands the uploader to use a custom image processor class located in the module path")

args = parser.parse_args()
data_path = args.data_path
dataset_name = args.dataset_name
hypercube, custom_metadata_parser, custom_image_processor = False, False, False
module_path = ''

if data_path is not None and dataset_name is not None:
    # validate args
    if data_path.strip() is "":
        print "Data path is empty"
        quit()

    if dataset_name.strip() is "":
        print "Dataset name is empty"
        quit()
    
    if args.hypercube is not None and str(args.hypercube).strip() is not '':
        hypercube = args.hypercube

    parser_class, image_processor_impl = None, None

    if args.module_path is not None and args.module_path.strip() is not '':
        # module-path arg must be specified if custom classes are used
        module_path = args.module_path

        # add the new module path to sys path
        print module_path
        sys.path.append(module_path)
        print sys.path

        from importlib import import_module

        if args.custom_metadata_parser is not None and args.custom_metadata_parser == True:
            # user must have a submodule called 'custom_metadata_parser' containing a class
            # 'CustomMetadataParser' that implements the MetadataParser ABC
            cls = getattr(import_module('custom_metadata_parser'), 'CustomMetadataParser')
            parser_class = cls

        if args.custom_image_processor is not None and args.custom_image_processor == True:
            # user must have a submodule called 'custom_image_processor' containing a class
            # 'CustomImageProcessor' that implements the ImageProcessor ABC 
            cls = getattr(import_module('custom_image_processor'), 'CustomImageProcessor')
            image_processor_impl = cls

    if parser_class is None:
        from omero_metadata_parser.aggregate_metadata import MetadataAggregator as parser_class

    if image_processor_impl is None:
        from omero_data_transfer.default_image_processor import DefaultImageProcessor as image_processor_impl

    with open(CONFIG_FILE, 'r') as cfg:
        CONFIG = yaml.load(cfg, Loader=yaml.FullLoader)

    conn_settings = CONFIG['test_settings']['omero_conn']
    broker = OMERODataBroker(username=conn_settings['username'],
                             password=conn_settings['password'],
                             host=conn_settings['server'],
                             port=conn_settings['port'],
                             image_processor=image_processor_impl())
    broker.open_omero_session()

    # dir_path = os.path.join("","/var","data_dir")
    # dir_path = os.path.join(PROJECT_DIR,"..","Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")

    data_transfer_manager = DataTransferManager(parser_class=parser_class)
    data_transfer_manager.upload_data_dir(broker, data_path, hypercube=hypercube)
    # upload_metadata(broker, dir_path)
    broker.close_omero_session()
