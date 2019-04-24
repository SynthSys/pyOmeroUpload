#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import omero
import sys

__author__ = "Johnny Hay"
__copyright__ = "Johnny Hay"
__license__ = "mit"

# https://pypi.org/project/pytest-docker-compose/


class OmeroTest:
    client, session = None, None

    def __init__(self):
        pass

    @pytest.fixture(scope="class")
    def omero_session(self):
        client = omero.client("localhost", 4064)
        # client = omero.client("demo.openmicroscopy.org", 4064)

        # client._optSetProp(id, "IceSSL.Ciphers", "ADH:@SECLEVEL=0")
        # client._optSetProp(id, "IceSSL.Ciphers", "ADH:!LOW:!MD5:!EXP:!3DES:@STRENGTH")

        # login to create a session (Service Factory)
        session = client.createSession("test", "test")
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

    """@classmethod
    def setup_class(self):
        # append the OMERO Server Python dependencies into PYTHONPATH - not working, need to do in env var :(
        # sys.path.append("/home/jhay/Downloads/OMERO.py-5.4.10-ice36-b105/lib/python")

        # client = omero.client("omeroserver.org")
        self.client = omero.client("localhost", 4064)
        # client = omero.client("demo.openmicroscopy.org", 4064)

        # client._optSetProp(id, "IceSSL.Ciphers", "ADH:@SECLEVEL=0")
        # client._optSetProp(id, "IceSSL.Ciphers", "ADH:!LOW:!MD5:!EXP:!3DES:@STRENGTH")

        # login to create a session (Service Factory)
        self.session = self.client.createSession("test", "test")
        # session = client.createSession("jhay", "cooCheegh4qu")

        # get a Stateless Service
        #queryService = session.getQueryService()
        # get a local omero.model.DatasetI
        #dataset = queryService.get("Dataset", 1)
        #print dataset  # see what's loaded
        #print dataset.name.val  # rtypes same as getName().getValue()
        #print dataset.copyImageLinks()  # omero.UnloadedEntityException!

    @classmethod
    def teardown_class(self):
        cls.client.closeSession()  # Free server resourses"""



