#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

# override installed pyOmeroUpload package
import sys
sys.path.insert(1, '/home/jovyan/work/pyOmeroUpload/src')

import os
import argparse
import getpass
import yaml
from omero_data_transfer.data_transfer_manager import DataTransferManager
from omero_data_transfer.omero_data_broker import OMERODataBroker
from pyomero_upload.pyomero_upload import PyOmeroUploader

# Instantiate the parser
parser = argparse.ArgumentParser(description='PyOmeroUpload Data Transfer Application')

# one or the other mandatory config args
# user must either provide a config file or set of connection params
parser.add_argument('-c', '--config-file', dest='config_file',
    type=str, required=False, metavar='config-file',
    help="specifies the system file path to the configuration file containing connection parameters")

# set of connection params
parser.add_argument('-u', '--username', dest='username',
    type=str, required=False, metavar='username',
    help="specifies the username for connection to the remote OMERO server")

parser.add_argument('-s', '--server', dest='server',
    type=str, required=False, metavar='server',
    help="specifies the server name of the remote OMERO server to connect")

parser.add_argument('-o', '--port', dest='port', nargs='?',
    const=4064, type=int, required=False, metavar='port',
    help="specifies the port on the remote OMERO server to connect (default is 4064)")

parser.add_argument('-a', '--password', dest='password',
    action='store_true',
    help="hidden password prompt for connection to the remote OMERO server")

# mandatory args
parser.add_argument('-d', '--data-path', dest='data_path',
    type=str, required=True, metavar='data-path',
    help="specifies the system file path to the data directory for uploading")

parser.add_argument('-n', '--dataset-name', dest='dataset_name',
    type=str, required=True, metavar='dataset-name',
    help="specifies the name of the destination dataset")

# optional args
parser.add_argument('-y', '--hypercube', action='store_true',
    dest='hypercube',required=False,
    help="commands the uploader to generate hypercube images")

parser.add_argument('-m', '--module-path', dest='module_path',
    type=str, required=False, metavar='module-path',
    help="specifies the system file path to the directory containing custom classes")

parser.add_argument('-p', '--custom-metadata-parser', action='store_true',
    dest='custom_metadata_parser', required=False,
    help="commands the uploader to use a custom parser class located in the module path")

parser.add_argument('-i', '--custom-image-processor', action='store_true',
    dest='custom_image_processor', required=False,
    help="commands the uploader to use a custom image processor class located in the module path")

args = parser.parse_args()
data_path = args.data_path
dataset_name = args.dataset_name
config_file = args.config_file
hypercube, custom_metadata_parser, custom_image_processor = False, False, False
module_path = ''

username = args.username
server = args.server
USERNAME, PASSWORD, HOST, PORT = '', '', '', 0

if config_file is not None:
    # validate args
    if config_file.strip() is "":
        print "Configuration file is empty"
        quit()

    with open(config_file, 'r') as cfg:
        config = yaml.load(cfg, Loader=yaml.FullLoader)

    USERNAME = config['omero_conn']['username']
    PASSWORD = config['omero_conn']['password']
    HOST = config['omero_conn']['server']
    PORT = config['omero_conn']['port']
elif username is not None and server is not None:
    # validate args
    if username.strip() is "":
        print "Username is empty"
        quit()

    if server.strip() is "":
        print "Dataset name is empty"
        quit()

    PASSWORD = str(getpass.getpass())
    USERNAME = username
    HOST = server
    PORT = args.port

# validate args
if USERNAME.strip() is "":
    print "Username is empty"
    quit()

if HOST.strip() is "":
    print "Target OMERO server is empty"
    quit()

if PASSWORD.strip() is "":
    print "Password is empty"
    quit()

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
        sys.path.append(module_path)

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

    # override `parser_class` for custom metadata extractor implementations
    if parser_class is None:
        from omero_metadata_parser.aggregate_metadata import MetadataAggregator as parser_class

    # override `image_processor_impl` for custom image processing implementations
    if image_processor_impl is None:
        from omero_data_transfer.default_image_processor import DefaultImageProcessor as image_processor_impl

    # initialise the PyOmeroUploader
    uploader = PyOmeroUploader(username=USERNAME, password=PASSWORD, server=HOST, port=PORT)

    # start upload process
    uploader.launch_upload(dataset_name=dataset_name, data_path=data_path, hypercube=hypercube,\
        parser_class=parser_class, image_processor_impl=image_processor_impl)
