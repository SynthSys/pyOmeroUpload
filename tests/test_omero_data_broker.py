#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import omero
import sys
from omero_data_transfer.omero_data_broker import OMERODataType, OMERODataBroker
# import omero.sys as ome_sys
from omero.rtypes import *

__author__ = "Johnny Hay"
__copyright__ = "Johnny Hay"
__license__ = "mit"

# https://pypi.org/project/pytest-docker-compose/

TEST_SETTINGS = {
    "omero_server": "localhost",
    "omero_port": 4064,
    "username": "test",
    "password": "test"
}


class TestOmeroDataBroker:
    client, session = None, None

    @pytest.fixture(scope="class")
    def omero_data_broker(self):
        '''Returns a default OMERO data broker for localhost'''
        return  OMERODataBroker(username=TEST_SETTINGS['username'], password=TEST_SETTINGS['password'],
                             host=TEST_SETTINGS['omero_server'], port=TEST_SETTINGS['omero_port'])

    @pytest.fixture(scope="class")
    def omero_session(self, omero_data_broker):
        broker =  OMERODataBroker(username=TEST_SETTINGS['username'], password=TEST_SETTINGS['password'],
                             host=TEST_SETTINGS['omero_server'], port=TEST_SETTINGS['omero_port'])
        print omero_data_broker.HOST
        omero_data_broker.open_omero_session()
        print("teardown smtp")
        # broker.close_omero_session()

    def test_get_connection(self, omero_data_broker):
        conn = omero_data_broker.get_connection()
        assert conn.canCreate() is True
        conn.close()

    def test_create_dataset(self, omero_data_broker):
        omero_data_broker.open_omero_session()
        dataset_obj = omero_data_broker.create_dataset("Test Dataset")
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
        for project in projects:
            print project
        omero_data_broker.close_omero_session()

    def test_get_datasets(self, omero_data_broker):
        omero_data_broker.open_omero_session()
        # my_exp_id = omero_data_broker.SESSION.getUser().getId()
        # default_group_id = omero_data_broker.SESSION.getEventContext().groupId

        conn = omero_data_broker.get_connection()
        my_exp_id = conn.getUser().getId()
        default_group_id = conn.getEventContext().groupId

        opts = {'owner': my_exp_id,  'group': default_group_id,
                'order_by': 'lower(obj.name)', 'limit': 5, 'offset': 0}
        datasets = omero_data_broker.retrieve_objects(OMERODataType.dataset, opts)
        for dataset in datasets:
            print dataset
        omero_data_broker.close_omero_session()

    def test_get_images(self, omero_data_broker):
        omero_data_broker.open_omero_session()
        # my_exp_id = omero_data_broker.SESSION.getUser().getId()
        # default_group_id = omero_data_broker.SESSION.getEventContext().groupId

        conn = omero_data_broker.get_connection()
        my_exp_id = conn.getUser().getId()
        default_group_id = conn.getEventContext().groupId

        print my_exp_id
        print default_group_id

        opts = {'annotator': my_exp_id,  'group': default_group_id,
               'experimenter': my_exp_id, 'order_by': 'lower(obj.name)', 'limit': 5,
                'leaves': True, 'offset': 0}
        opts = {'experimenter':'root', 'group': 'system'}
        opts = dict() #{'experimenter': 0, 'group': 0}
        opts['experimenter'] = rint(0L)
        # opts['group'] = rint(0)
        # opts['experimenter'] = 0L
        # opts['group'] = 0L
        # params = ome_sys.Parameters(opts)

        params = omero.sys.Parameters()
        # params.map = {"experimenter": rlong(long(0)), 'group': rlong(long(0))}
        params.map = {"experimenter": rlong(my_exp_id), "group": rlong(default_group_id)}
        params.experimenter = rlong(0)
        # params.map = {}
        # params.map['group'] = rlong(long(0))

        images = omero_data_broker.retrieve_objects(OMERODataType.image, params)
        for image in images:
            print image
        omero_data_broker.close_omero_session()

    def test_query_project(self, omero_data_broker):
        omero_data_broker.open_omero_session()
        projectId = 101
        params = omero.sys.Parameters()
        params.map = {"pid": rlong(projectId)}

        project = omero_data_broker.query_projects(params)
        print project
        # for project in projects:
        #     print project
        omero_data_broker.close_omero_session()