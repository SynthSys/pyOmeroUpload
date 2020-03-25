#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from omero_data_transfer.omero_data_broker import OMERODataType, OMERODataBroker
import os,sys,inspect

# make src directory accessible to tests
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.join(os.path.dirname(currentdir), 'src')
# sys.path.insert(0,parentdir)

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"


class TestOmeroDataBroker:
    client, session = None, None

    @pytest.fixture(scope="class")
    def omero_data_broker(self):
        CONFIG = {}
        CONFIG['username'] = 'test'
        CONFIG['password'] = 'test'
        CONFIG['server'] = 'localhost'
        CONFIG['port'] =  4064
        '''Returns a default OMERO data broker for localhost'''
        return OMERODataBroker(CONFIG)

    @pytest.fixture(scope="class")
    def omero_session(self, omero_data_broker):
        CONFIG = {}
        CONFIG['username'] = 'test'
        CONFIG['password'] = 'test'
        CONFIG['server'] = 'localhost'
        CONFIG['port'] =  4064
        broker = OMERODataBroker(CONFIG)
        omero_data_broker.open_omero_session()
        print("teardown smtp")
        # broker.close_omero_session()

    def test_get_connection(self, omero_data_broker):
        conn = omero_data_broker.get_connection()
        assert conn.canCreate() is True
        conn.close()

    def test_create_dataset(self, omero_data_broker):
        omero_data_broker.open_omero_session()
        dataset_obj = omero_data_broker.create_dataset()
        assert hasattr(dataset_obj, 'id')
        print "New dataset, Id:", dataset_obj.getId().getValue()
        omero_data_broker.close_omero_session()

    def test_get_projects(self, omero_data_broker):
        omero_data_broker.open_omero_session()
        # my_exp_id = omero_data_broker.SESSION.getUser().getId()
        # default_group_id = omero_data_broker.SESSION.getEventContext().groupId

        conn = omero_data_broker.get_connection()
        my_exp_id = conn.getUser().getId()
        default_group_id = conn.getEventContext().groupId

        opts = {'owner': my_exp_id,  'group': default_group_id,
                'order_by': 'lower(obj.name)', 'limit': 5, 'offset': 0}
        projects = omero_data_broker.retrieve_objects(OMERODataType.project, opts)

        if projects is not None:
            for project in projects:
                print project
        omero_data_broker.close_omero_session()
