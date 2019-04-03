#!/usr/bin/env python
# -*- coding: utf-8 -*-

from enum import Enum
from omero.gateway import BlitzGateway
from omero import client as om_client
from omero import model
from omero import rtypes
from omero import ClientError
import logging

logging.basicConfig()


class OMERODataType(Enum):
    project = 1
    dataset = 2
    image = 3

    def describe(self):
        # self is the member here
        return self.name, self.value

    def __str__(self):
        return self.name.capitalize()


class OMERODataBroker:

    # USERNAME, PASSWORD, HOST, PORT = None, None, None, None

    def __init__(self, username="", password="", host="",
                 port=0):
        self.USERNAME = username
        self.PASSWORD = password
        self.HOST = host
        self.PORT = port

        self.CLIENT = om_client(self.HOST, self.PORT)
        self.SESSION = None

    def open_omero_session(self):
        try:
            self.SESSION = self.CLIENT.getSession()
            return
        except ClientError:
            print "No live session"

        self.SESSION = self.CLIENT.createSession(self.USERNAME, self.PASSWORD)
        return

    def close_omero_session(self):
        self.CLIENT.closeSession()

    def get_connection(self):
        conn = BlitzGateway(self.USERNAME, self.PASSWORD, host=self.HOST,
                            port=self.PORT)
        conn.connect()

        # Using secure connection.
        # By default, once we have logged in, data transfer is not encrypted
        # (faster)
        # To use a secure connection, call setSecure(True):
        conn.setSecure(True)
        return conn

    def create_dataset(self):
        dataset_obj = model.DatasetI()
        dataset_obj.setName(rtypes.rstring("New Dataset"))
        dataset_obj = self.SESSION.getUpdateService().saveAndReturnObject(dataset_obj)
        dataset_id = dataset_obj.getId().getValue()
        return dataset_obj

    def retrieve_objects(self, data_type, opts):
        # my_exp_id = self.SESSION.getObjects(data_type, opts=opts)
        pass

def main():
    broker = OMERODataBroker(username="test",
                               password="test", host="localhost",
                               port=4064)
    broker.get_omero_session()
    broker.close_omero_session()

if __name__ == "__main__":
    main()