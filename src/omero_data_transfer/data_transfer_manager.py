#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from omero_data_transfer.omero_data_broker import OMERODataType, OMERODataBroker
import metadata_parser.extract_metadata  as parser

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")


def upload_data_dir(data_broker, dir_path):
    print "hello"
    metadata = None

    # retrieve the metadata from the log file
    for exp_file in os.listdir(dir_path):
        print exp_file
        if exp_file.endswith("log.txt"):  # the log file containing metadata
            metadata = parser.extract_metadata(os.path.join(dir_path, exp_file))
            break

    print metadata

    # create the dataset using metadata values
    data_broker.open_omero_session()
    dataset_obj = data_broker.create_dataset(metadata["aim"])
    data_broker.close_omero_session()
    print dataset_obj

    # upload data (image) files
    for root, sub_dirs, files in os.walk(dir_path):
        print sub_dirs

        for sub_dir in sub_dirs:
            for data_file in os.listdir((os.path.join(dir_path, sub_dir))):
                print data_file
                # TODO: call data broker (which in turn calls Java upload image method)


def main():
    broker = OMERODataBroker(username="test",
                           password="test", host="localhost",
                           port=4064)

    print PROJECT_DIR
    dir_path = os.path.join(PROJECT_DIR, "tests", "test_data", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")
    # filename =""
    upload_data_dir(broker, dir_path)


if __name__ == "__main__":
    main()




