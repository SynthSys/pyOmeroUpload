#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

import os
from enum import Enum
from omero.gateway import BlitzGateway, TagAnnotationWrapper,\
    MapAnnotationWrapper
from omero import client as om_client
from omero import model, grid
from omero import rtypes
from omero import ClientError
from omero import sys
from omero import constants
from omero import gateway
from omero import cli as om_cli
import omero.util.script_utils as script_utils
from PIL import Image
import numpy as np
from threading import Thread, BoundedSemaphore
from multiprocessing.pool import ThreadPool
from functools import partial
from image_processor import ImageProcessor
from omero_data_transfer.default_image_processor import DefaultImageProcessor
from omero import java as om_java
import subprocess

class OMERODataType(Enum):
    project = 1
    dataset = 2
    image = 3

    def describe(self):
        # self is the member here
        return self.name, self.value

    def __str__(self):
        return self.name.capitalize()


JAVA_BIN_PATH = "/usr/local/openjdk-8/bin/java"
JAVA_CLASS_PATH = "-classpath /home/jovyan/work/OMERO.server-5.4.10-ice36-b105/lib/server/*"

# Need to override the default OMERO java.py function since it cannot find the java binary
def popen(args,
          java=JAVA_BIN_PATH,
          xargs=JAVA_CLASS_PATH,
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

    def __init__(self, username="", password="", host="",
                 port=0, image_processor=DefaultImageProcessor()):
        self.USERNAME = username
        self.PASSWORD = password
        self.HOST = host
        self.PORT = port

        self.CLIENT = om_client(self.HOST, self.PORT)
        self.SESSION = None
        self.IMAGE_PROCESSOR = image_processor

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

    def destroy_omero_session(self):
        self.CLIENT.destroySession(self.CLIENT.getSessionId())

    def get_connection(self):
        extra_config = dict()
        # extra_config["omero.dump"] = "1"
        # extra_config["omero.client.viewer.roi_limit"] = "100"
        extra_config = "../../tests/ice.config"
        conn = BlitzGateway(self.USERNAME, self.PASSWORD, host=self.HOST,
                            port=self.PORT, extra_config=extra_config)
        conn.connect()

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

        print resources.areTablesEnabled()

        repository_id = resources.repositories().descriptions[0].getId().getValue()
        print repository_id
        #for des in resources.repositories().descriptions:
            #print des
            #print des._id

        # create columns and data types, and populate with data
        data_types = dataframe.dtypes
        table_data = []
        init_cols = []

        # for  index, col in enumerate(columns):
            #col_type = data_types.loc[index]
            # col_data = dataframe

        for index, col in enumerate(dataframe.columns):
            print index, col, dataframe[col].dtype

            if dataframe[col].dtype == object:
                max_len = dataframe[col].str.len().max()
                init_col = grid.StringColumn(col, '', max_len, [])
                init_cols.append(init_col)

                data_col = grid.StringColumn(col, '', max_len, list(dataframe.iloc[:, index].values))
                table_data.append(data_col)

        # print table_data

        table = resources.newTable(repository_id, ''.join(["/", table_name, ".h5"]))
        # table = resources.newTable(dataset_id, table_name)
        table.initialize(init_cols)
        table.addData(table_data)

        '''ids = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        strings = ["one", "two", "three", "four", "five",
                   "six", "seven", "eight", "nine", "ten"]
        data1 = grid.LongColumn('Uid', 'test Long', ids)
        data2 = grid.StringColumn('MyStringColumn', '', 64, strings)
        data = [data1, data2]
        table.addData(data)'''

        # note that this worked after the table.close() statement was invoked in 5.4.10
        orig_file = table.getOriginalFile()
        table.close()  # when we are done, close.

        orig_file_id = orig_file.id.val
        print orig_file_id
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

    def upload_image(self, file_to_upload, dataset, import_original=True):
        valid_image = False
        file_mime_type = None

        if file_to_upload.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff')):
            import filetype
            ftype = filetype.guess(file_to_upload)
            if ftype is None:
                print('Cannot guess file type!')
                valid_image = False

            print('File extension: %s' % ftype.extension)
            print('File MIME type: %s' % ftype.mime)

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
            print filename_w_ext

            if import_original==True:
                # use the function that follows if uploading images as original files (i.e. as imports)
                conn = gateway.BlitzGateway(client_obj=self.CLIENT)

                '''
                original_file = conn.createOriginalFileFromLocalFile(localPath = file_to_upload,
                                            origFilePathAndName = filename_w_ext,
                                            mimetype=file_mime_type, ns=None)
                '''
                cli = om_cli.CLI()
                cli.loadplugins()
                cli.set_client(self.CLIENT)

                os.environ["JAVA_HOME"] = "/usr/local/openjdk-8"
                os.environ["PATH"] = "$PATH:$JAVA_HOME/bin"

                om_java.popen = popen
                om_java.check_java(["/usr/local/openjdk-8/bin/java"])
                print "found java"
                #command = java.cmd(args, java, xargs, chdir, debug, debug_string)
                # check_java(command)
                if dataset:
                    target = "Dataset:id:" + str(dataset.id.getValue())
                    cli.onecmd(["import", "--clientdir", "/home/jovyan/work/OMERO.server-5.4.10-ice36-b105/lib/client",
                                '-T', target, '--description', "an image",
                                '--no-upgrade-check', file_to_upload])
                else:
                    cli.onecmd(["import", "--clientdir", "/home/jovyan/work/OMERO.server-5.4.10-ice36-b105/lib/client",
                                '--description', "an image", '--no-upgrade-check', file_to_upload])
            else:
                planes = script_utils.getPlaneFromImage(imagePath=file_to_upload, rgbIndex=None)

                # Use below function if uploading images in RawPixelsStore format (i.e. not the original file import)
                script_utils.createNewImage(self.SESSION, [planes], filename, "An image", dataset)

    def upload_images(self, files_to_upload, dataset_id=None, hypercube=False):
        dataset = None
        query_service = self.SESSION.getQueryService()

        if dataset_id != None:
            params = dict()
            params["Data_Type"] = "Dataset"
            params["IDs"] = str(dataset_id)
            # conn = BlitzGateway(client_obj=self.CLIENT)
            # dataset = script_utils.get_objects(conn, params)
            # query = "select p from Project p left outer join fetch p.datasetLinks as links left outer join fetch links.child as dataset where p.id =:pid"
            query = 'select d from Dataset d where d.id = :did'

            params = sys.Parameters()
            params.map = {"did": rtypes.rlong(dataset_id)}
            dataset = query_service.findByQuery(query, params)

            print dataset.getId().getValue()

        pool = ThreadPool(processes=10)

        if hypercube == True:
            # assume each sub-folder in the path contains one hypercube
            # should retrieve each position's folder
            '''
            common_path = os.path.commonprefix(files_to_upload)
            print common_path

            import glob
            cube_dirs = glob.glob(''.join([common_path,'*']))

            update_service = self.SESSION.getUpdateService()
            pixels_service = self.SESSION.getPixelsService()

            for path in cube_dirs:
                self.upload_dir_as_images(query_service, update_service, pixels_service,
                                     path, dataset, convert_to_uint16=True)
            '''
            self.IMAGE_PROCESSOR.process_images(self.SESSION, files_to_upload, dataset)
        else:
            # initialise the upload_image function with the current dataset
            # since the pool.map function won't accept multiple arguments
            cur_upload_image = partial(self.upload_image, dataset=dataset)
            pool.map(cur_upload_image, files_to_upload)


    # object_type = 'Dataset', 'Image', 'Project'
    def add_tags(self, tag_values, object_type, object_id):
        # self.SESSION.
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
    CONFIG_FILE = os.path.join(PROJECT_DIR, 'config.yml')
    CONFIG = {}

    with open(CONFIG_FILE, 'r') as cfg:
        CONFIG = yaml.load(cfg, Loader=yaml.FullLoader)

    conn_settings = CONFIG['test_settings']['omero_conn']

    broker = OMERODataBroker(username=conn_settings['username'],
                             password=conn_settings['password'],
                             host=conn_settings['server'], port=conn_settings['port'],
                             image_processor=DefaultImageProcessor())
    broker.open_omero_session()
    broker.close_omero_session()


if __name__ == "__main__":
    main()