#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

from omero_data_transfer.data_transfer_manager import DataTransferManager
from omero_data_transfer.omero_data_broker import OMERODataBroker
from omero_metadata_parser.aggregate_metadata import MetadataAggregator
from omero_data_transfer.default_image_processor import DefaultImageProcessor


class PyOmeroUploader:

    def __init__(self, username, password, server, port=4064):
        self.USERNAME = username
        self.PASSWORD = password
        self.SERVER = server
        self.PORT = port

    # initialise broker and manager with the given parameters and start the upload process 
    def launch_upload(self, dataset_name, data_path, hypercube=False,
                      parser_class=MetadataAggregator, image_processor_impl=DefaultImageProcessor):

        # override `parser_class` for custom metadata extractor implementations
        if parser_class is None:
            from omero_metadata_parser.aggregate_metadata import MetadataAggregator as parser_class

        # override `image_processor_impl` for custom image processing implementations
        if image_processor_impl is None:
            from omero_data_transfer.default_image_processor import DefaultImageProcessor as image_processor_impl

        # conn_settings = config['omero_conn']
        broker = OMERODataBroker(username=self.USERNAME, password=self.PASSWORD, server=self.SERVER, port=self.PORT,
                                 image_processor=image_processor_impl())
        broker.open_omero_session()

        data_transfer_manager = DataTransferManager(parser_class=parser_class)
        dataset_id = data_transfer_manager.upload_data_dir(broker, dataset_name, data_path, hypercube=hypercube)

        # upload_metadata(broker, dir_path)
        broker.close_omero_session()

        print ': '.join('Uploaded Dataset ID', str(dataset_id))

        dataset_url = '/'.join('http:/', self.SERVER, 'webclient', '-'.join('?show=dataset', str(dataset_id)))
        print ': '.join('Uploaded Dataset URL', dataset_url)
