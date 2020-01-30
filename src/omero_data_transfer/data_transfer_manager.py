#!/usr/bin/env python
# -*- coding: utf-8 -*-

# override installed pyOmeroUpload package
import sys
sys.path.insert(1, '/home/jovyan/work/pyOmeroUpload2/src')
print sys.path

import os
import glob
from omero_data_transfer.omero_data_broker import OMERODataBroker
import subprocess
import yaml
from omero_data_transfer.default_image_processor import DefaultImageProcessor
from metadata_parser.extract_log_metadata import LogMetadataParser
from metadata_parser.extract_acq_metadata import AcqMetadataParser

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


def generate_cli_args(files_to_upload, dirs_to_upload, dataset):
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


def get_str_array_val(metadata_val):
    str_val = ''

    if isinstance(metadata_val, list):
        str_val = metadata_val[0]
    elif isinstance(metadata_val, str):
        str_val = metadata_val

    return str_val


# construct the KVPs strings from the main key (e.g. 'Brightfield') and the
# metadata values, so all KVPs are returned as an array of pairs arrays
def build_kvps(kvp_key, metadata_kvps):
    kvp_list = []
    cur_kvp_key = kvp_key
    cur_kvp_val = ''

    for idx, kvp_str in enumerate(metadata_kvps):
        print kvp_str

        # check if there's a colon; if not, assume it's the value of the current key
        # if it contains a colon, it must be a KVP
        kvps = kvp_str.split(':')
        print kvps

        if len(kvps) == 1:
            if len(cur_kvp_val.strip()) == 0:
                cur_kvp_val = kvps[0]
            else:
                cur_kvp_val = ';'.join([cur_kvp_val, get_str_array_val(kvp_str.strip())])
        elif len(kvps) > 1:
            # append the current KVP into the list
            kvp_list.append([cur_kvp_key, get_str_array_val(cur_kvp_val)])

            for i in range(0, len(kvps)/2):
                cur_kvp_key = kvps[0+i]
                cur_kvp_val = kvps[1+i]

                kvp_list.append([cur_kvp_key, get_str_array_val(cur_kvp_val)])

    print kvp_list

    # if len(kvp_list) == 0:
    #     kvp_list = None

    return kvp_list


def upload_metadata(dataset_id, data_broker, dir_path, log_metadata):
    input_path = glob.glob(os.path.join(dir_path,'*Acq.txt'))
    print input_path

    # handle acquisition metadata file parsing
    # input_file = open(input_path[0])
    acq_parser = AcqMetadataParser()
    acq_metadata = acq_parser.extract_metadata(input_path[0])

    input_path = glob.glob(os.path.join(dir_path,'*log.txt'))
    print input_path

    # handle acquisition metadata file parsing
    # log_metadata = log_parser.extract_log_metadata(input_path[0])
    print acq_metadata

    data_broker.open_omero_session()

    # add tags to dataset
    data_broker.add_tags(log_metadata.tags, 'Dataset', dataset_id)

    # create tables as file annotation attachments
    for key in acq_metadata.table_dict:
        table = data_broker.create_table(dataset_id, table_dict[key], key)

    # do key:value pairs
    kvp_list = log_metadata.kvp_list

    print kvp_list
    data_broker.add_kvps(kvp_list, 'Dataset', dataset_id)
    # data_broker.add_kvps(log_metadata.brightfield, 'Dataset', dataset_id)
    # data_broker.add_kvps(log_metadata.dic, 'Dataset', dataset_id)
    # data_broker.add_kvps(log_metadata.gfp, 'Dataset', dataset_id)
    # data_broker.add_kvps(log_metadata.gfpfast, 'Dataset', dataset_id)
    # data_broker.add_kvps(log_metadata.cy5, 'Dataset', dataset_id)

    # data_broker.add_kvps(acq_metadata.switch_params, 'Dataset', dataset_id)

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


def upload_data_dir(data_broker, dir_path, import_images=True):
    exp_file, log_metadata = None, None

    # retrieve the metadata from the log file
    for exp_file in os.listdir(dir_path):
        if exp_file.endswith("log.txt"):  # the log file containing metadata
            break

    print dir_path, exp_file
    log_parser = LogMetadataParser()
    log_metadata = log_parser.extract_metadata(os.path.join(dir_path, exp_file))
    # create the dataset using metadata values
    print dir(log_metadata)
    dataset_name = log_metadata.aim
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
        upload_metadata(dataset_id, data_broker, dir_path, log_metadata)

        # if the image data files are to be imported as is in their existing format,
        # use the Java jar client with CLI arguments
        if import_images == True:
            args = generate_cli_args(files_to_upload, dirs_to_upload, dataset_id)
            print args

            command = ['java', '-jar', str(CLIENT_JAR_PATH)]
            command.extend(args)

            subprocess.call(command)
        else:
            data_broker.open_omero_session()

            data_broker.upload_images(files_to_upload, dataset_id, hypercube=True)

            data_broker.close_omero_session()

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
                             image_processor=DefaultImageProcessor())
    print "hello"
    broker.open_omero_session()

    # dir_path = os.path.join("","/var","data_dir")
    dir_path = os.path.join(PROJECT_DIR,"..","Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")
    upload_data_dir(broker, dir_path, import_images=False)
    # upload_metadata(broker, dir_path)
    print "hello2"
    broker.close_omero_session()


if __name__ == "__main__":
    main()




