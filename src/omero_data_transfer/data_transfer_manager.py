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
from omero_data_transfer.omero_data_broker import OMERODataBroker
import subprocess
import yaml
from omero_data_transfer.default_image_processor import DefaultImageProcessor as image_processor_impl
from omero_metadata_parser.aggregate_metadata import MetadataAggregator as metadata_parser_impl
from omero_metadata_parser.metadata_parser import MetadataParser

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")

# alter below CLIENT_TYPE var to switch between executable jars
#CLIENT_TYPE = "importer"
CLIENT_TYPE = "cli"

CLIENT_JAR_NAME = ".".join(["-".join(["omero", CLIENT_TYPE]), "jar"])
CLIENT_JAR_PATH = os.path.join(PROJECT_DIR, 'resources', CLIENT_JAR_NAME)
CONFIG_FILE = os.path.join(PROJECT_DIR, 'config.yml')
CONFIG = {}

with open(CONFIG_FILE, 'r') as cfg:
    CONFIG = yaml.load(cfg, Loader=yaml.FullLoader)

class DataTransferManager:
    metadata_parser = metadata_parser_impl()

    def __init__(self, parser_class=None):
        if parser_class is not None:
            self.metadata_parser = parser_class()

    def generate_cli_args(self, files_to_upload, dirs_to_upload, dataset):
        conn_settings = CONFIG['test_settings']['omero_conn']
        args = list()

        if CLIENT_TYPE == 'importer':
            files_arg = ",".join([str(x) for x in files_to_upload])
            dirs_arg = ",".join([str(x) for x in dirs_to_upload])

            args = ['-h', conn_settings['server'], '-p', str(conn_settings['port']),
                    '-u', conn_settings['username'], '-w', conn_settings['password'],
                    '-f', files_arg, '-d', dirs_arg, '-s', dataset]
        else:
            '''
                Invoke the official OMERO CommandLineImporter Java application. This
                program takes one file or folder at a time; in the case of folders, they are
                recusively scanned for all images in sub-directories and included in the
                upload
            '''
            # -s localhost -u user -w password -d Dataset:50 foo.tiff
            data_arg = None

            if dirs_to_upload is not None:
                data_arg = os.path.commonprefix(dirs_to_upload)
            elif files_to_upload is not None:
                data_arg = " ".join([str(x) for x in files_to_upload])

            args = ['-s', conn_settings['server'], '-p', str(conn_settings['port']),
                    '-u', conn_settings['username'], '-w', conn_settings['password'],
                    '-d', dataset, data_arg] #'--depth', '7', items_arg]

        return args

    def upload_metadata(self, dataset_id, data_broker, dir_path, metadata):
        data_broker.open_omero_session()

        # add tags to dataset
        data_broker.add_tags(metadata.tags, 'Dataset', dataset_id)

        # create tables as file annotation attachments
        for key in metadata.table_dict:
            table = data_broker.create_table(dataset_id, metadata.table_dict[key], key)

        # do key:value pairs
        kvp_list = metadata.kvp_list

        data_broker.add_kvps(kvp_list, 'Dataset', dataset_id)

        data_broker.close_omero_session()

    '''
    This function uses a data broker object to create a dataset based on metadata
    extracted from the microscopy log files in the specified directory. The dataset
    will be named using these metadata and created in the configured OMERO 
    server. At the moment, it's using a custom-built Java client for uploading 
    images, but it could be switched to use the official OMERO CLI which does 
    essentially the same thing 
    (https://docs.openmicroscopy.org/omero/5.4.10/users/cli/import.html).
    '''
    def upload_data_dir(self, data_broker, dir_path, hypercube=False):
        exp_file, log_metadata = None, None

        # retrieve the metadata from the log file
        for exp_file in os.listdir(dir_path):
            if exp_file.endswith("log.txt"):  # the log file containing metadata
                break

        print dir_path, exp_file
        #metadata_parser = metadata_parser_impl()
        metadata = self.metadata_parser.extract_metadata(dir_path)
        # create the dataset using metadata values
        print dir(metadata)
        dataset_name = metadata.aim
        print dataset_name

        try:
            data_broker.open_omero_session()
            dataset_obj = data_broker.create_dataset(dataset_name)
            data_broker.close_omero_session()

            dirs_to_upload = []
            files_to_upload = []

            # upload data (image) files
            for root, sub_dirs, files in os.walk(dir_path):
                dirs_to_upload.append(root)

                for sub_dir in sub_dirs:

                    dirs_to_upload.append(os.path.join(root, sub_dir))

                    for data_file in os.listdir((os.path.join(dir_path, sub_dir))):
                        files_to_upload.append(os.path.join(root, sub_dir, data_file))

            dataset_id = str(dataset_obj.getId().getValue())
            print dataset_id
            self.upload_metadata(dataset_id, data_broker, dir_path, metadata)

            data_broker.open_omero_session()

            data_broker.upload_images(files_to_upload, dataset_id, hypercube)

            data_broker.close_omero_session()
            '''
            # if the image data files are to be imported as is in their existing format,
            # use the Java jar client with CLI arguments
            if hypercube == False:

                args = self.generate_cli_args(files_to_upload, dirs_to_upload, dataset_id)
                print args

                command = ['java', '-jar', str(CLIENT_JAR_PATH)]
                command.extend(args)

                subprocess.call(command)
            else:
                data_broker.open_omero_session()

                data_broker.upload_images(files_to_upload, dataset_id, hypercube=True)

                data_broker.close_omero_session()
            '''

        except Exception as error:
            print(error)
        finally:
            data_broker.close_omero_session()


def main():
    conn_settings = CONFIG['test_settings']['omero_conn']
    broker = OMERODataBroker(username=conn_settings['username'],
                             password=conn_settings['password'],
                             host=conn_settings['server'],
                             port=conn_settings['port'],
                             image_processor=image_processor_impl())
    print "hello"
    broker.open_omero_session()

    # dir_path = os.path.join("","/var","data_dir")
    dir_path = os.path.join(PROJECT_DIR,"..","Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")

    data_transfer_manager = DataTransferManager()
    data_transfer_manager.upload_data_dir(broker, dir_path, hypercube=False)
    # upload_metadata(broker, dir_path)
    print "hello2"
    broker.close_omero_session()


if __name__ == "__main__":
    main()




