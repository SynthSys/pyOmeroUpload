#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import omero
import sys

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


class OmeroTest:
    client, session = None, None

    def __init__(self):
        pass

    @pytest.fixture(scope="class")
    def omero_session(self):
        client = omero.client(TEST_SETTINGS['omero_server'], TEST_SETTINGS['omero_port'])
        # client = omero.client("demo.openmicroscopy.org", 4064)

        # client._optSetProp(id, "IceSSL.Ciphers", "ADH:@SECLEVEL=0")
        # client._optSetProp(id, "IceSSL.Ciphers", "ADH:!LOW:!MD5:!EXP:!3DES:@STRENGTH")

        # login to create a session (Service Factory)
        session = client.createSession(TEST_SETTINGS['username'], TEST_SETTINGS['password'])
        print('hello')
        yield session  # provide the fixture value
        print("teardown smtp")
        client.closeSession()

    def test_create_dataset(self, omero_session):
        dataset_obj = omero.model.DatasetI()
        dataset_obj.setName("New Dataset")
        dataset_obj = omero_session.getUpdateService().saveAndReturnObject(dataset_obj)
        dataset_id = dataset_obj.getId().getValue()
        print "New dataset, Id:", dataset_id
        x = "hello"
        assert hasattr(x, 'check')



