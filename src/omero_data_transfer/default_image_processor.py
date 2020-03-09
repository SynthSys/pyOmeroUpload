#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

import sys
sys.path.insert(1, '/home/jovyan/work/pyOmeroUpload/src')

from omero_data_transfer.image_processor import ImageProcessor
import glob
import abc
import os
from omero import sys
from omero import rtypes
from omero import model
import numpy as np
import omero.util.script_utils as script_utils
import logging
import re
from operator import itemgetter

# logging config
logging.basicConfig(filename='image_processing.log', level=logging.DEBUG)
PROCESSING_LOG = logging.getLogger(__name__)
# handlers
f_handler = logging.FileHandler('image_processing.log')
f_handler.setLevel(logging.DEBUG)
f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
f_handler.setFormatter(f_format)
PROCESSING_LOG.addHandler(f_handler)

ACCEPTED_MIME_TYPES = ['image/jpeg', 'image/jpx', 'image/png', 'image/gif', 'image/webp', 'image/x-canon-cr2',
                       'image/tiff', 'image/bmp', 'image/vnd.ms-photo', 'image/vnd.adobe.photoshop', 'image/x-icon',
                       'image/heic']


class DefaultImageProcessor(ImageProcessor):

    def process_images(self, omero_session, file_path, dataset=None, convert_to_uint16=True):
        common_path = os.path.commonprefix(file_path)

        common_path = re.sub(r'(.*)pos[0-9]+[^$]', '\\1', common_path)

        cube_dirs = [f for f in os.listdir(common_path) if re.search(r'pos[0-9]+$', f)]

        query_service = omero_session.getQueryService()
        update_service = omero_session.getUpdateService()
        pixels_service = omero_session.getPixelsService()

        hypercube_ids = []

        for path in cube_dirs:
            path = os.path.join(common_path, path)
            if os.path.isdir(path) == True:
                image_id = self.upload_dir_as_images(omero_session, query_service, update_service, pixels_service,
                                        path, dataset, convert_to_uint16)

                if image_id is not None:
                    hypercube_ids.append(image_id)

        return hypercube_ids


    # adapted from script_utils
    def upload_dir_as_images(self, omero_session, query_service, update_service,
                          pixels_service, path, dataset=None, convert_to_uint16=False):
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

            if f.lower().endswith(('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff')):
                import filetype
                ftype = filetype.guess(fullpath)
                if ftype is None:
                    PROCESSING_LOG.error('Cannot guess file type!')
                    continue

                PROCESSING_LOG.debug('File extension: %s' % ftype.extension)
                PROCESSING_LOG.debug('File MIME type: %s' % ftype.mime)

                if ftype.mime not in ACCEPTED_MIME_TYPES:
                    continue

            search_res = self.run_regex_search(fullpath, f)
            tSearch, cSearch, zSearch, tokSearch, posSearch = \
                itemgetter('tSearch', 'cSearch', 'zSearch', 'tokSearch', 'posSearch')(search_res)

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

        chans_map = self.find_channel_map(rgb, channelSet)
        channels, colourMap = itemgetter('channels', 'colourMap')(chans_map)

        sizeC = len(channels)

        # use the common stem as the image name
        # imageName = os.path.commonprefix(tokens).strip('0T_')
        # imageName = os.path.commonprefix(tokens).strip('_00')
        imageName = pos
        description = "Imported from images in %s" % path
        PROCESSING_LOG.info("Creating image: %s" % imageName)

        # use the last image to get X, Y sizes and pixel type
        if rgb:
            plane = script_utils.getPlaneFromImage(fullpath, 0)
        else:
            plane = script_utils.getPlaneFromImage(fullpath)

        pixelsType = self.get_pixels_type(plane, query_service, convert_to_uint16)

        sizeY, sizeX = plane.shape

        PROCESSING_LOG.debug("sizeX: %s  sizeY: %s sizeZ: %s  sizeC: %s  sizeT: %s"
                     % (sizeX, sizeY, sizeZ, sizeC, sizeT))

        # code below here is very similar to combineImages.py
        # create an image in OMERO and populate the planes with numpy 2D arrays
        channelList = range(sizeC)
        imageId = pixels_service.createImage(
            sizeX, sizeY, sizeZ, sizeT, channelList,
            pixelsType, imageName, description)

        pixelsId = self.upload_image_pixels(imageId, query_service, omero_session,
            pixels_service, rgb, imageMap, sizeC, sizeZ, sizeT, sizeX, sizeY,
            channels, colourMap, convert_to_uint16)

        # add channel names
        pixels = pixels_service.retrievePixDescription(pixelsId)
        i = 0
        # c is an instance of omero.model.ChannelI
        for c in pixels.iterateChannels():
            # returns omero.model.LogicalChannelI
            lc = c.getLogicalChannel()
            lc.setName(rtypes.rstring(channels[i]))
            update_service.saveObject(lc)
            i += 1

        # put the image in dataset, if specified.
        if dataset:
            link = model.DatasetImageLinkI()
            link.parent = model.DatasetI(dataset.id.val, False)
            link.child = model.ImageI(imageId, False)
            update_service.saveAndReturnObject(link)

        return imageId

    def run_regex_search(self, full_path, file):
        regex_token = re.compile(r'(?P<Token>.+)\.')
        # regex_time = re.compile(r'T(?P<T>\d+)')
        regex_time = re.compile(r'.*_(?P<T>\d+)_\w+\d*_\d+\.')
        # regex_channel = re.compile(r'_C(?P<C>.+?)(_|$)')
        regex_channel = re.compile(r'.*_\d+_(?P<C>\w+\d*)_\d+\.')
        # regex_zslice = re.compile(r'_Z(?P<Z>\d+)')
        regex_zslice = re.compile(r'.*_\d+_\w+\d*_(?P<Z>\d+)\.')
        regex_pos = re.compile(r'.*/(?P<pos>pos\d+)/.*')

        tSearch = regex_time.search(file)
        cSearch = regex_channel.search(file)
        zSearch = regex_zslice.search(file)
        tokSearch = regex_token.search(file)
        posSearch = regex_pos.search(full_path)

        return {'tSearch': tSearch,
                'cSearch': cSearch,
                'zSearch': zSearch,
                'tokSearch': tokSearch,
                'posSearch': posSearch}

    def find_channel_map(self, rgb, channelSet):
        channels = []
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

        return {'channels': channels,
                'colourMap': colourMap}

    def get_pixels_type(self, plane, query_service, convert_to_uint16=False):
        pType = plane.dtype.name
        # look up the PixelsType object from DB
        # omero::model::PixelsType
        pixelsType = query_service.findByQuery(
            "from PixelsType as p where p.value='%s'" % pType, None)
        if pixelsType is None and pType.startswith("float"):  # e.g. float32
            # omero::model::PixelsType
            pixelsType = query_service.findByQuery(
                "from PixelsType as p where p.value='%s'" % script_utils.PixelsTypefloat, None)
        if pixelsType is None:
            PROCESSING_LOG.warn("Unknown pixels type for: %s" % pType)
            return

        if convert_to_uint16 == True:
            query = "from PixelsType as p where p.value='uint16'"
            pixelsType = query_service.findByQuery(query, None)

        return pixelsType
    
    def upload_image_pixels(self, imageId, query_service, omero_session,
        pixels_service, rgb, imageMap, sizeC, sizeZ, sizeT, sizeX, sizeY,
        channels, colourMap, convert_to_uint16=False):
        params = sys.ParametersI()
        params.addId(imageId)
        pixelsId = query_service.projection(
            "select p.id from Image i join i.pixels p where i.id = :id",
            params)[0][0].val

        rawPixelStore = omero_session.createRawPixelsStore()
        rawPixelStore.setPixelsId(pixelsId, True)

        zStart = 1  # could be 0 or 1 ?
        tStart = 1

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
                                PROCESSING_LOG.debug(
                                    "Getting rgb plane from: %s" % imagePath)
                                plane2D = script_utils.getPlaneFromImage(imagePath, theC)
                            else:
                                PROCESSING_LOG.debug("Getting plane from: %s" % imagePath)
                                plane2D = script_utils.getPlaneFromImage(imagePath)
                        else:
                            PROCESSING_LOG.debug(
                                "Creating blank plane for .",
                                theZ, channels[theC], theT)
                            plane2D = np.zeros((sizeY, sizeX))
                        PROCESSING_LOG.debug(
                            "Uploading plane: theZ: %s, theC: %s, theT: %s"
                            % (theZ, theC, theT))

                        if convert_to_uint16 == True:
                            plane2D = np.array(plane2D, dtype=np.uint16)

                        script_utils.upload_plane(rawPixelStore, plane2D, theZ, theC, theT)
                        minValue = min(minValue, plane2D.min())
                        maxValue = max(maxValue, plane2D.max())
                pixels_service.setChannelGlobalMinMax(
                    pixelsId, theC, float(minValue), float(maxValue))
                rgba = None
                if theC in colourMap:
                    rgba = colourMap[theC]
                try:
                    renderingEngine = omero_session.createRenderingEngine()
                    script_utils.resetRenderingSettings(
                        renderingEngine, pixelsId, theC, minValue, maxValue, rgba)
                finally:
                    renderingEngine.close()
        finally:
            rawPixelStore.close()
        
        return pixelsId