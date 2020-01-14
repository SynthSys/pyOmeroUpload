#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import omero.util.script_utils as script_utils
from PIL import Image
import numpy as np
import logging
from threading import Thread, BoundedSemaphore
from multiprocessing.pool import ThreadPool
from functools import partial

SU_LOG = logging.getLogger("omero.util.script_utils")
logging.basicConfig(filename='example.log',level=logging.DEBUG)
logging.debug('This message should go to the log file')
logging.info('So should this')
logging.warning('And this, too')


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
        for des in resources.repositories().descriptions:
            print des
            print des._id

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

        print table_data

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
        table.close()  # when we are done, close.

        orig_file = table.getOriginalFile()
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

    def upload_image(self, file_to_upload, dataset):
        # convert image to 2DArray for plane
        im = Image.open(file_to_upload)

        imarray = np.array(im)
        print imarray

        filename_w_ext = os.path.basename(file_to_upload)
        filename, file_extension = os.path.splitext(filename_w_ext)
        print filename_w_ext

        script_utils.createNewImage(self.SESSION, [imarray], filename,
                                    "An image", dataset)

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

            print dataset

        pool = ThreadPool(processes=10)

        if hypercube == True:
            # assume each sub-folder in the path contains one hypercube
            # should retrieve each position's folder
            common_path = os.path.commonprefix(files_to_upload)
            print common_path

            import glob
            cube_dirs = glob.glob(''.join([common_path,'*']))

            update_service = self.SESSION.getUpdateService()
            pixels_service = self.SESSION.getPixelsService()

            for path in cube_dirs:
                self.upload_dir_as_images(query_service, update_service, pixels_service,
                                     path, dataset, convert_to_uint16=True)
        else:
            # initialise the upload_image function with the current dataset
            # since the pool.map function won't accept multiple arguments
            cur_upload_image = partial(self.upload_image, dataset=dataset)
            pool.map(cur_upload_image, files_to_upload)

    # adapted from script_utils
    def upload_dir_as_images(self, queryService, updateService,
                          pixelsService, path, dataset=None, convert_to_uint16=False):
        """
        Reads all the images in the directory specified by 'path' and
        uploads them to OMERO as a single
        multi-dimensional image, placed in the specified 'dataset'
        Uses regex to determine the Z, C, T position of each image by name,
        and therefore determines sizeZ, sizeC, sizeT of the new Image.

        @param path     the path to the directory containing images.
        @param dataset  the OMERO dataset, if we want to put images somewhere.
                        omero.model.DatasetI
        """
        import re

        regex_token = re.compile(r'(?P<Token>.+)\.')
        # regex_time = re.compile(r'T(?P<T>\d+)')
        regex_time = re.compile(r'.*_(?P<T>\d+)_\w+\d*_\d+\.')
        # regex_channel = re.compile(r'_C(?P<C>.+?)(_|$)')
        regex_channel = re.compile(r'.*_\d+_(?P<C>\w+\d*)_\d+\.')
        # regex_zslice = re.compile(r'_Z(?P<Z>\d+)')
        regex_zslice = re.compile(r'.*_\d+_\w+\d*_(?P<Z>\d+)\.')
        regex_pos = re.compile(r'.*/(?P<pos>pos\d+)/.*')

        # assume 1 image in this folder for now.
        # Make a single map of all images. key is (z,c,t). Value is image path.
        imageMap = {}
        channelSet = set()
        tokens = []

        # other parameters we need to determine
        sizeZ = 1
        sizeC = 1
        sizeT = 1
        zStart = 1  # could be 0 or 1 ?
        tStart = 1

        fullpath = None

        rgb = False
        # process the names and populate our imagemap
        for f in os.listdir(path):
            fullpath = os.path.join(path, f)
            tSearch = regex_time.search(f)
            cSearch = regex_channel.search(f)
            zSearch = regex_zslice.search(f)
            tokSearch = regex_token.search(f)
            posSearch = regex_pos.search(fullpath)
            pos = posSearch.group('pos')

            if f.endswith(".jpg"):
                rgb = True

            if tSearch is None:
                theT = 0
            else:
                theT = int(tSearch.group('T'))

            if cSearch is None:
                cName = "0"
            else:
                cName = cSearch.group('C')

            if zSearch is None:
                theZ = 0
            else:
                theZ = int(zSearch.group('Z'))

            channelSet.add(cName)
            sizeZ = max(sizeZ, theZ)
            zStart = min(zStart, theZ)
            sizeT = max(sizeT, theT)
            tStart = min(tStart, theT)
            if tokSearch is not None:
                tokens.append(tokSearch.group('Token'))
            imageMap[(theZ, cName, theT)] = fullpath

        colourMap = {}
        if not rgb:
            channels = list(channelSet)
            # see if we can guess what colour the channels should be, based on
            # name.
            for i, c in enumerate(channels):
                if c == 'rfp':
                    colourMap[i] = script_utils.COLOURS["Red"]
                if c == 'gfp':
                    colourMap[i] = script_utils.COLOURS["Green"]
        else:
            channels = ("red", "green", "blue")
            colourMap[0] = script_utils.COLOURS["Red"]
            colourMap[1] = script_utils.COLOURS["Green"]
            colourMap[2] = script_utils.COLOURS["Blue"]

        sizeC = len(channels)

        # use the common stem as the image name
        # imageName = os.path.commonprefix(tokens).strip('0T_')
        # imageName = os.path.commonprefix(tokens).strip('_00')
        imageName = pos
        description = "Imported from images in %s" % path
        SU_LOG.info("Creating image: %s" % imageName)

        # use the last image to get X, Y sizes and pixel type
        if rgb:
            plane = script_utils.getPlaneFromImage(fullpath, 0)
        else:
            plane = script_utils.getPlaneFromImage(fullpath)
        pType = plane.dtype.name
        # look up the PixelsType object from DB
        # omero::model::PixelsType
        pixelsType = queryService.findByQuery(
            "from PixelsType as p where p.value='%s'" % pType, None)
        if pixelsType is None and pType.startswith("float"):  # e.g. float32
            # omero::model::PixelsType
            pixelsType = queryService.findByQuery(
                "from PixelsType as p where p.value='%s'" % script_utils.PixelsTypefloat, None)
        if pixelsType is None:
            SU_LOG.warn("Unknown pixels type for: %s" % pType)
            return
        sizeY, sizeX = plane.shape

        SU_LOG.debug("sizeX: %s  sizeY: %s sizeZ: %s  sizeC: %s  sizeT: %s"
                     % (sizeX, sizeY, sizeZ, sizeC, sizeT))

        if convert_to_uint16 == True:
            query = "from PixelsType as p where p.value='uint16'"
            pixelsType = queryService.findByQuery(query, None)

        # code below here is very similar to combineImages.py
        # create an image in OMERO and populate the planes with numpy 2D arrays
        channelList = range(sizeC)
        imageId = pixelsService.createImage(
            sizeX, sizeY, sizeZ, sizeT, channelList,
            pixelsType, imageName, description)
        params = sys.ParametersI()
        params.addId(imageId)
        pixelsId = queryService.projection(
            "select p.id from Image i join i.pixels p where i.id = :id",
            params)[0][0].val

        rawPixelStore = self.SESSION.createRawPixelsStore()
        rawPixelStore.setPixelsId(pixelsId, True)
        try:
            for theC in range(sizeC):
                minValue = 0
                maxValue = 0
                for theZ in range(sizeZ):
                    zIndex = theZ + zStart
                    for theT in range(sizeT):
                        tIndex = theT + tStart
                        if rgb:
                            c = "0"
                        else:
                            c = channels[theC]
                        if (zIndex, c, tIndex) in imageMap:
                            imagePath = imageMap[(zIndex, c, tIndex)]
                            if rgb:
                                SU_LOG.debug(
                                    "Getting rgb plane from: %s" % imagePath)
                                plane2D = script_utils.getPlaneFromImage(imagePath, theC)
                            else:
                                SU_LOG.debug("Getting plane from: %s" % imagePath)
                                plane2D = script_utils.getPlaneFromImage(imagePath)
                        else:
                            SU_LOG.debug(
                                "Creating blank plane for .",
                                theZ, channels[theC], theT)
                            plane2D = np.zeros((sizeY, sizeX))
                        SU_LOG.debug(
                            "Uploading plane: theZ: %s, theC: %s, theT: %s"
                            % (theZ, theC, theT))

                        if convert_to_uint16 == True:
                            plane2D = np.array(plane2D, dtype=np.uint16)

                        script_utils.upload_plane(rawPixelStore, plane2D, theZ, theC, theT)
                        minValue = min(minValue, plane2D.min())
                        maxValue = max(maxValue, plane2D.max())
                pixelsService.setChannelGlobalMinMax(
                    pixelsId, theC, float(minValue), float(maxValue))
                rgba = None
                if theC in colourMap:
                    rgba = colourMap[theC]
                try:
                    renderingEngine = self.SESSION.createRenderingEngine()
                    script_utils.resetRenderingSettings(
                        renderingEngine, pixelsId, theC, minValue, maxValue, rgba)
                finally:
                    renderingEngine.close()
        finally:
            rawPixelStore.close()

        # add channel names
        pixels = pixelsService.retrievePixDescription(pixelsId)
        i = 0
        # c is an instance of omero.model.ChannelI
        for c in pixels.iterateChannels():
            # returns omero.model.LogicalChannelI
            lc = c.getLogicalChannel()
            lc.setName(rtypes.rstring(channels[i]))
            updateService.saveObject(lc)
            i += 1

        # put the image in dataset, if specified.
        if dataset:
            link = model.DatasetImageLinkI()
            link.parent = model.DatasetI(dataset.id.val, False)
            link.child = model.ImageI(imageId, False)
            updateService.saveAndReturnObject(link)

        return imageId

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
                             host=conn_settings['server'], port=conn_settings['port'])
    broker.open_omero_session()
    broker.close_omero_session()


if __name__ == "__main__":
    main()