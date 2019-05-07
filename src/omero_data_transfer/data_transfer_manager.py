#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from omero_data_transfer.omero_data_broker import OMERODataBroker
import metadata_parser.extract_metadata  as parser
import subprocess

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")
CLIENT_JAR_PATH = os.path.join(PROJECT_DIR, 'resources',
                        'omero_test_harness-1.0-SNAPSHOT.jar')

SETTINGS = {
    "omero_server": "localhost",
    "omero_port": 4064,
    "username": "test",
    "password": "test"
}


'''
This function uses a data broker object to create a dataset based on metadata
extracted from the microscopy log files in the specified directory. The dataset
will be named using these metadata and created in the configured OMERO 
server. At the moment, it's using a custom-built Java client for uploading 
images, but it could be switched to use the official OMERO CLI which does 
essentially the same thing 
(https://docs.openmicroscopy.org/omero/5.4.10/users/cli/import.html).
'''
def upload_data_dir(data_broker, dir_path):
    metadata = None

    # retrieve the metadata from the log file
    for exp_file in os.listdir(dir_path):
        if exp_file.endswith("log.txt"):  # the log file containing metadata
            metadata = parser.extract_metadata(os.path.join(dir_path, exp_file))
            break

    # create the dataset using metadata values
    data_broker.open_omero_session()
    dataset_name = metadata["aim"]
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

    files_arg = ",".join([str(x) for x in files_to_upload])
    dirs_arg = ",".join([str(x) for x in dirs_to_upload])

    args = ['-h', SETTINGS['omero_server'], '-p', str(SETTINGS['omero_port']),
            '-u', SETTINGS['username'], '-w', SETTINGS['password'],
            '-f', files_arg, '-d', dirs_arg, '-s', str(dataset_obj.getId().getValue())]

    command = ['java', '-jar', str(CLIENT_JAR_PATH)]
    command.extend(args)

    subprocess.call(command)


def main():
    broker = OMERODataBroker(username=SETTINGS['username'], password=SETTINGS['password'],
                             host=SETTINGS['omero_server'], port=SETTINGS['omero_port'])

    dir_path = os.path.join(PROJECT_DIR, "tests", "test_data", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")
    upload_data_dir(broker, dir_path)


if __name__ == "__main__":
    main()




