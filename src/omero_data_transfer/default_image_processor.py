from image_processor import ImageProcessor
import glob
import abc

class DefaultImageProcessor(ImageProcessor):

    def process_images(self, omero_session, file_path, dataset=None, convert_to_uint16=True):
        update_service = self.SESSION.getUpdateService()
        pixels_service = self.SESSION.getPixelsService()

        common_path = os.path.commonprefix(file_path)
        print common_path

        cube_dirs = glob.glob(''.join([common_path,'*']))

        query_service = self.SESSION.getQueryService()
        update_service = self.SESSION.getUpdateService()
        pixels_service = self.SESSION.getPixelsService()

        for path in cube_dirs:
            self.upload_dir_as_images(query_service, update_service, pixels_service,
                                    path, dataset, convert_to_uint16)


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