#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

# override installed pyOmeroUpload package
import sys
sys.path.insert(1, '/home/jovyan/work/pyOmeroUpload2/src')
print sys.path

import pytest
import os
import yaml
import glob
import omero.util.script_utils as script_utils
from omero_data_transfer.omero_data_broker import OMERODataBroker
from omero_data_transfer.default_image_processor import DefaultImageProcessor as image_processor_impl

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")

CONFIG_FILE = os.path.join(PROJECT_DIR, 'config.yml')
CONFIG = {}

with open(CONFIG_FILE, 'r') as cfg:
    CONFIG = yaml.load(cfg, Loader=yaml.FullLoader)


def test_run_regex_search():
    image_processor = image_processor_impl()

    dir_path = os.path.join(PROJECT_DIR,"..","Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")

    for f in os.listdir(dir_path):
        fullpath = os.path.join(dir_path, f)

        search_res = image_processor.run_regex_search(fullpath, f)


def test_find_channel_map():
    image_processor = image_processor_impl()

    rgb = False     # non-jpeg
    channelSet = set()

    cName = 0
    channelSet.add(cName)

    chans_map = image_processor.find_channel_map(rgb, channelSet)


def test_get_pixels_type():
    image_processor = image_processor_impl()

    conn_settings = CONFIG['test_settings']['omero_conn']
    broker = OMERODataBroker(username=conn_settings['username'],
                             password=conn_settings['password'],
                             host=conn_settings['server'],
                             port=conn_settings['port'],
                             image_processor=image_processor_impl())
    print "hello"
    broker.open_omero_session()

    query_service = broker.SESSION.getQueryService()
    convert_to_uint16 = True

    dir_path = os.path.join(PROJECT_DIR,"..","Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")

    common_path = os.path.commonprefix(dir_path)
    print common_path

    cube_dirs = glob.glob(''.join([common_path,'pos???']))

    for path in cube_dirs:
        if os.path.isdir(path) == True:

            for f in os.listdir(path):
                fullpath = os.path.join(path, f)

                plane = script_utils.getPlaneFromImage(fullpath)

                pixelsType = image_processor.get_pixels_type(plane, query_service, convert_to_uint16)