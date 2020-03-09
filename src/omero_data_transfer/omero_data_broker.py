#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

import sys
sys.path.insert(1, '/home/jovyan/work/pyOmeroUpload/src')

import os
from enum import Enum
from omero.gateway import BlitzGateway, TagAnnotationWrapper, \
    MapAnnotationWrapper
from omero import client as om_client
from omero import model, grid
from omero import rtypes
from omero import ClientError
from omero import sys
from omero import constants
from omero import gateway
from omero import cli as om_cli
from omero import java as om_java
import omero.util.script_utils as script_utils
import logging
from PIL import Image
import numpy as np
from threading import Thread, BoundedSemaphore
from multiprocessing.pool import ThreadPool
from functools import partial
from image_processor import ImageProcessor
from omero_data_transfer.default_image_processor import DefaultImageProcessor
import subprocess

# logging config
logging.basicConfig(filename='omero_upload.log', level=logging.DEBUG)
BROKER_LOG = logging.getLogger(__name__)
# handlers
f_handler = logging.FileHandler('omero_upload.log')
f_handler.setLevel(logging.DEBUG)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
BROKER_LOG.addHandler(f_handler)


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
    # see HQL query examples at
    # https://docs.openmicroscopy.org/omero/5.4.10/developers/GettingStarted.html
    # and model/table names at
    # https://docs.openmicroscopy.org/omero/5.4.3/developers/Model/EveryObject.html
    LINKED_ANNOS_BY_PROJ_QUERY = "select p from Project p left outer join " \
                                 "fetch p.annotationLinks as links left outer join fetch " \
                                 "links.child as annotation where p.id=:pid"
    LINKED_ANNOS_BY_DS_QUERY = "select d from Dataset d left outer join " \
                               "fetch d.annotationLinks as links left outer join fetch " \
                               "links.child as annotation where d.id=:did"

    ACCEPTED_MIME_TYPES = ['image/jpeg', 'image/jpx', 'image/png', 'image/gif', 'image/webp', 'image/x-canon-cr2',
                           'image/tiff', 'image/bmp', 'image/vnd.ms-photo', 'image/vnd.adobe.photoshop', 'image/x-icon',
                           'image/heic']

    def __init__(self, username, password, server, port=4064,
                 image_processor=DefaultImageProcessor(), ice_config=None,
                 java_bin_path=None, java_class_path=None):

        self.USERNAME = username
        self.PASSWORD = password
        self.HOST = server
        self.PORT = port

        self.ICE_CONFIG = ice_config

        self.CLIENT = om_client(self.HOST, self.PORT)
        self.SESSION = None
        self.IMAGE_PROCESSOR = image_processor

        # Need to override the default OMERO java.py function since it cannot find the java binary
        def popen(args,
                  java=java_bin_path,
                  xargs=java_class_path,
                  chdir=None,
                  debug=None,
                  debug_string=om_java.DEFAULT_DEBUG,
                  stdout=subprocess.PIPE,
                  stderr=subprocess.PIPE):
            """
            Creates a subprocess.Popen object and returns it. Uses cmd() internally
            to create the Java command to be executed. This is the same logic as
            run(use_exec=False) but the Popen is returned rather than the stdout.
            """
            from omero import java as om_java
            command = om_java.cmd(args, java, xargs, chdir, debug, debug_string)

            om_java.check_java(command)
            if not chdir:
                chdir = os.getcwd()
            return subprocess.Popen(command, stdout=stdout, stderr=stderr,
                                    cwd=chdir, env=os.environ)

        om_java.popen = popen

    def open_omero_session(self):
        try:
            self.SESSION = self.CLIENT.getSession()
            return
        except ClientError:
            BROKER_LOG.info("No live session")

        self.SESSION = self.CLIENT.createSession(self.USERNAME, self.PASSWORD)
        return

    def close_omero_session(self):
        self.CLIENT.closeSession()

    def destroy_omero_session(self):
        self.CLIENT.destroySession(self.CLIENT.getSessionId())

    def get_connection(self):
        ice_config = "/dev/null"
        if self.ICE_CONFIG is not None:
            ice_config = self.ICE_CONFIG

        c = om_client(host=self.HOST, port=self.PORT,
                      args=["=".join("--Ice.Config", ice_config), "--omero.debug=1"])
        c.createSession(self.USERNAME, self.PASSWORD)
        conn = BlitzGateway(client_obj=c)

        # Using secure connection.
        # By default, once we have logged in, data transfer is not encrypted
        # (faster)
        # To use a secure connection, call setSecure(True):
        conn.setSecure(True)
        return conn

    def create_dataset(self, dataset_name):
        dataset_obj = model.DatasetI()
        dataset_obj.setName(rtypes.rstring(dataset_name))
        dataset_obj = self.SESSION.getUpdateService().saveAndReturnObject(dataset_obj)
        dataset_id = dataset_obj.getId().getValue()
        return dataset_obj

    def retrieve_objects(self, data_type, ids=None, opts=None):
        objects = list()
        if data_type == OMERODataType.project:
            objects = self.SESSION.getContainerService().loadContainerHierarchy(
                "Project", ids, opts)
        elif data_type == OMERODataType.dataset:
            objects = self.SESSION.getContainerService().loadContainerHierarchy(
                "Dataset", ids, opts)
        elif data_type == OMERODataType.image:
            objects = self.SESSION.getContainerService().getUserImages(options=opts)

        return objects

    def load_object_annotations(self, data_type, object_id):
        annos = list()
        if data_type == OMERODataType.project:
            params = sys.Parameters()
            params.map = {"pid": rtypes.rlong(object_id)}
            annos = self.find_objects_by_query(self.LINKED_ANNOS_BY_PROJ_QUERY,
                                               params)
        elif data_type == OMERODataType.dataset:
            params = sys.Parameters()
            params.map = {"did": rtypes.rlong(object_id)}
            annos = self.find_objects_by_query(self.LINKED_ANNOS_BY_DS_QUERY,
                                               params)

        return annos

    def find_objects_by_query(self, query, params):
        queryService = self.SESSION.getQueryService()
        # query = "select p from Project p left outer join fetch p.datasetLinks as links left outer join fetch links.child as dataset where p.id =:pid"

        objects = queryService.findByQuery(query, params)

        return objects

    def query_projects(self, params):
        queryService = self.SESSION.getQueryService()
        # query = "select p from Project p left outer join fetch p.datasetLinks as links left outer join fetch links.child as dataset where p.id =:pid"
        query = 'select p from Project p where p.id = :pid'

        project = queryService.findByQuery(query, params)

        return project

    def create_table(self, dataset_id, dataframe, table_name):
        columns = dataframe.columns
        resources = self.SESSION.sharedResources()
        # resources = conn.c.sf.sharedResources()

        repository_id = resources.repositories().descriptions[0].getId().getValue()

        # create columns and data types, and populate with data
        data_types = dataframe.dtypes
        table_data = []
        init_cols = []

        for index, col in enumerate(dataframe.columns):
            if dataframe[col].dtype == object:
                max_len = dataframe[col].str.len().max()
                init_col = grid.StringColumn(col, '', max_len, [])
                init_cols.append(init_col)

                data_col = grid.StringColumn(col, '', max_len, list(dataframe.iloc[:, index].values))
                table_data.append(data_col)

        table = resources.newTable(repository_id, ''.join(["/", table_name, ".h5"]))
        # table = resources.newTable(dataset_id, table_name)
        table.initialize(init_cols)
        table.addData(table_data)

        # note that this worked after the table.close() statement was invoked in 5.4.10
        orig_file = table.getOriginalFile()
        table.close()  # when we are done, close.

        orig_file_id = orig_file.id.val

        # ...so you can attach this data to an object e.g. Dataset
        file_ann = model.FileAnnotationI()
        # use unloaded OriginalFileI
        file_ann.setFile(model.OriginalFileI(orig_file_id, False))
        file_ann = self.SESSION.getUpdateService().saveAndReturnObject(file_ann)

        link = model.DatasetAnnotationLinkI()
        link.setParent(model.DatasetI(dataset_id, False))
        link.setChild(model.FileAnnotationI(file_ann.getId().getValue(), False))
        table = self.SESSION.getUpdateService().saveAndReturnObject(link)

        return table

    def upload_image(self, file_to_upload, dataset, import_original=True, cli=None):
        valid_image = False
        file_mime_type = None
        image = None

        if file_to_upload.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff')):
            import filetype
            ftype = filetype.guess(file_to_upload)
            if ftype is None:
                BROKER_LOG.error('Cannot guess file type!')
                valid_image = False

            BROKER_LOG.debug('File extension: %s' % ftype.extension)
            BROKER_LOG.debug('File MIME type: %s' % ftype.mime)

            if ftype.mime not in self.ACCEPTED_MIME_TYPES:
                valid_image = False
            else:
                valid_image = True
                file_mime_type = ftype.mime
        else:
            valid_image = False

        if valid_image == True:
            # convert image to 2DArray for plane
            im = Image.open(file_to_upload)
            # planes = [np.array(im)]

            filename_w_ext = os.path.basename(file_to_upload)
            filename, file_extension = os.path.splitext(filename_w_ext)

            if import_original == True and cli is not None:
                # use the function that follows if uploading images as original files (i.e. as imports)
                # conn = gateway.BlitzGateway(client_obj=self.CLIENT)
                # conn = self.get_connection()
                if dataset:
                    target = "Dataset:id:" + str(dataset.id.getValue())
                    cli.onecmd(["import", "--clientdir", "/home/jovyan/work/OMERO.server-5.4.10-ice36-b105/lib/client",
                                '-T', target, '--description', "an image", "--quiet",
                                '--no-upgrade-check', file_to_upload])
                else:
                    cli.onecmd(["import", "--clientdir", "/home/jovyan/work/OMERO.server-5.4.10-ice36-b105/lib/client",
                                '--description', "an image", '--no-upgrade-check', "--quiet", file_to_upload])
            else:
                planes = script_utils.getPlaneFromImage(imagePath=file_to_upload, rgbIndex=None)

                # Use below function if uploading images in RawPixelsStore format (i.e. not the original file import)
                image = script_utils.createNewImage(self.SESSION, [planes], filename, "An image", dataset)

        if image is not None:
            return image.getId().getValue()
        else:
            return

    def upload_images(self, files_to_upload, dataset_id=None, hypercube=True, import_original=False):
        dataset = None
        query_service = self.SESSION.getQueryService()
        image_ids = []

        if dataset_id != None:
            params = dict()
            params["Data_Type"] = "Dataset"
            params["IDs"] = str(dataset_id)
            query = 'select d from Dataset d where d.id = :did'

            params = sys.Parameters()
            params.map = {"did": rtypes.rlong(dataset_id)}
            dataset = query_service.findByQuery(query, params)

        if hypercube == True:
            image_ids = self.IMAGE_PROCESSOR.process_images(self.SESSION, files_to_upload, dataset)
        else:
            if import_original == True:
                # initialise the upload_image function with the current dataset
                # since the pool.map function won't accept multiple arguments
                conn = self.get_connection()
                if self.ICE_CONFIG is not None:
                    os.environ["ICE_CONFIG"] = self.ICE_CONFIG

                om_java.os.environ = os.environ.copy()

                cli = om_cli.CLI()
                cli.loadplugins()
                cli.set_client(conn.c)

                cur_upload_image = partial(self.upload_image, dataset=dataset, import_original=True, cli=cli)
                pool = ThreadPool(processes=10)

                results = pool.map(cur_upload_image, files_to_upload)
                pool.terminate()
                pool.close()

                cli.onecmd(["logout"])

                conn.c.closeSession()
                conn.close()
                cli.get_client().closeSession()
                cli.get_client().close()
                cli.conn().close()
                cli.close()
                cli.exit()
            else:
                cur_upload_image = partial(self.upload_image, dataset=dataset, import_original=False)
                pool = ThreadPool(processes=10)

                image_ids = pool.map(cur_upload_image, files_to_upload)
                pool.terminate()
                pool.close()

        return image_ids

    def add_tags(self, tag_values, object_type, object_id):
        update_service = self.SESSION.getUpdateService()

        for tag_value in tag_values:
            # tags shouldn't contain commas?
            split_tag_values = [x.strip() for x in tag_value.split(',')]

            # ignore empty tag strings
            for split_tag_value in split_tag_values:
                if len(split_tag_value.strip()) < 1:
                    continue

                # TODO: add check here to see if tag with specific value already exists and link it if so

                # use stateless update service instead of blitz
                new_tag_anno = model.TagAnnotationI()

                # TODO: determine what description the tag annotation should have; e.g. date, strain

                # Use 'client' namespace to allow editing in Insight & web
                new_tag_anno.setTextValue(rtypes.rstring(split_tag_value))

                tag_anno = update_service.saveAndReturnObject(new_tag_anno)

                # do link with parent object
                object = self.retrieve_objects(object_type, [object_id])

                link = None

                if object_type == str(OMERODataType.project):
                    link = model.ProjectAnnotationLinkI()
                    link.parent = model.ProjectI(object_id, False)
                elif object_type == str(OMERODataType.dataset):
                    link = model.DatasetAnnotationLinkI()
                    link.parent = model.DatasetI(object_id, False)
                elif object_type == str(OMERODataType.image):
                    link = model.ImageAnnotationLinkI()
                    link.parent = model.ImageI(object_id, False)

                link.child = model.TagAnnotationI(tag_anno.id, False)
                update_service.saveAndReturnObject(link)

    # Add key:value pairs: kvps param is array of pairs arrays
    def add_kvps(self, key_value_data, object_type, object_id):

        # use stateless update service instead of blitz
        new_map_anno = model.MapAnnotationI()

        # Use 'client' namespace to allow editing in Insight & web
        namespace = constants.metadata.NSCLIENTMAPANNOTATION
        new_map_anno.setNs(rtypes.rstring(namespace))

        # key_value_data = [{i[0]: i[1:]} for i in key_value_data]
        key_value_data = [model.NamedValue(i[0], str(i[1:])) for i in key_value_data]
        # key_value_data = [model.NamedValue(i[0], i[1:]) for i in key_value_data]
        new_map_anno.setMapValue(key_value_data)

        update_service = self.SESSION.getUpdateService()
        map_anno = update_service.saveAndReturnObject(new_map_anno)

        # do link with parent object
        object = self.retrieve_objects(object_type, [object_id])

        link = None

        if object_type == str(OMERODataType.project):
            link = model.ProjectAnnotationLinkI()
            link.parent = model.ProjectI(object_id, False)
        elif object_type == str(OMERODataType.dataset):
            link = model.DatasetAnnotationLinkI()
            link.parent = model.DatasetI(object_id, False)
        elif object_type == str(OMERODataType.image):
            link = model.ImageAnnotationLinkI()
            link.parent = model.ImageI(object_id, False)

        link.child = model.MapAnnotationI(map_anno.id, False)
        update_service.saveAndReturnObject(link)


def main():
    import yaml
    import os
    PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")
    CONFIG_FILE = os.path.join(PROJECT_DIR, 'config_test.yml')
    CONFIG = {}

    with open(CONFIG_FILE, 'r') as cfg:
        CONFIG = yaml.load(cfg, Loader=yaml.FullLoader)

    conn_settings = CONFIG['omero_conn']

    broker = OMERODataBroker(conn_settings,
                             image_processor=DefaultImageProcessor())
    broker.open_omero_session()
    broker.close_omero_session()


if __name__ == "__main__":
    main()
