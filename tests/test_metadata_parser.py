#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import pytest

import metadata_parser.extract_metadata  as parser

__author__ = "Johnny Hay"
__copyright__ = "Johnny Hay"
__license__ = "mit"

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..")

METADATA_CONFIG = {
    "start_tag": "Experiment details:",
    "attribute_tags": ["Aim:", "Strain:", "Comments:", "Brightfield:",
                               "DIC:", "GFP:", "Omero project:", "Omero tags:"],
    "end_tag": "Experiment started at:"
}


class TestMetadataParser:

    def test_extract_metadata(self):
        parser.METADATA_CONFIG = METADATA_CONFIG

        filename = os.path.join(PROJECT_DIR, "tests", "test_data",
                                "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00",
                                "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1log.txt")
        # filename =""
        metadata = parser.extract_metadata(filename)
        print metadata

        assert metadata["aim"] [0] == "Compare morphology markers across microscopes"
        assert metadata["omero project"][0] == "Morphology detection"
        assert metadata["omero tags"][0] == "28-Sep-2018,Batgirl,"
        assert metadata["omero tags"][1] == "PFS is locked"
        assert metadata["comments"][0] == "Low OD O/N. Log growth from OD0.1 for four hours."
        assert metadata["comments"][1] == "Microscope setup for used channels:"
        assert metadata["strain"][0] == "BY4741 Htb2-GFP"

        assert metadata["dic"].__len__() == 4
        assert metadata["gfp"].__len__() == 5
