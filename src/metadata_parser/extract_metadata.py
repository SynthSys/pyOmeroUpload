#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

"""
Metadata in a Swain Lab microscope OMERO record looks like:

    Experiment details:
    Aim: Compare morphology markers across microscopes
    Strain:
    BY4741 Htb2-GFP
    BY4741 Myo1-GFP
    BY4741 Hog1-GFP
    BY4741 Lte1-GFP
    BY4741 Vph1-GFP
    Comments:
    Low OD O/N. Log growth from OD0.1 for four hours.
    Microscope setup for used channels:
    Brightfield:
    "White LED
    ->(polarizer + prism + condenser)"]
    ->Filter block:["59022 exciter, 59022 dichroic, 535/50 emission filter]
    ->Emission filter wheel:[]
    DIC:
    "White LED
    ->(polarizer + prism + condenser)"]
    ->Filter block:["No exciter, No dichroic, Analyser]
    ->Emission filter wheel:[]
    GFP:
    "470nm LED
    ->Combiner cube:[ No exciter in combiner cube,  495lp dichroic-> (425lp dichroic)"]
    ->Filter block:["59022 exciter, 59022 dichroic, 535/50 emission filter]
    ->Emission filter wheel:[]
    Micromanager config file:C:\Users\Public\Microscope control\Micromanager config files\Batgirl5_9_18noemissionfilter.txt
    Omero project:
    Morphology detection
    Omero tags:
    28-Sep-2018,Batgirl,

So our metadata structure is:

    - Experiment details:
      - Aim:
      - Strain:
      - Comments:
      - Brightfield:
      - DIC:
      - GFP:
      - Omero project:
      - Omero tags:
"""

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")

METADATA_CONFIG = {
    "start_tag": "Experiment details:",
    "attribute_tags": ["Aim:", "Strain:", "Comments:", "Brightfield:",
                               "DIC:", "GFP:", "Omero project:", "Omero tags:"],
    "end_tag": "Experiment started at:"
}

"""
This function reads a Swain Lab microscope log file and extracts the metadata
from the experimental metadata block at the beginning. The metadata are 
collected in a dictionary, with each key-value-pair comprising the metadata tag
key and a list of attribute values. Essentially, each new line character within a
metadata attribute tag inserts a new item in the list of values, so there might
need to be some rules engine that specifies how to parse the actual metadata
attribute values depending on the tag? For example, perhaps 'omero tags' 
should be split into multiple items as a comma-separated list, while 'comments'
should be split based on new line chars. 
"""
def extract_metadata(filename):
    # init the metadata attributes dict
    metadata = dict()

    # add each attribute tag as a key in the dict
    for attr_tag in METADATA_CONFIG["attribute_tags"]:
        metadata[attr_tag.rstrip(":").lower()] = list()

    metadata_attr_tags = METADATA_CONFIG["attribute_tags"]
    f = open(filename)  # This is a big file

    is_metadata_block = False
    cur_metadata_attr = None

    # read each line in the file
    for line in f:

        # exit reading the file if we have reached the end of metadata block
        if line.startswith(METADATA_CONFIG["end_tag"]):
            is_metadata_block = False
            break

        if is_metadata_block:
            # if we're in the metadata block, check for a current attribute tag
            if cur_metadata_attr is None:
                # First check if the line is a new metadata attribute tag
                if line.strip() in metadata_attr_tags:
                    cur_metadata_attr = line.strip().rstrip(":").lower()
                else:
                    # next check if the line starts with a metadata attribute tag
                    for attr in metadata_attr_tags:
                        if line.startswith(attr):
                            cur_metadata_attr = attr.strip().rstrip(":").lower()

                            if not line.strip().endswith(attr):
                                # if line doesn't end with the attribute tag, there are values
                                # on the line and we assume it's a one line tag
                                metadata[cur_metadata_attr].append(line.strip().split(attr)[1].strip())
                                cur_metadata_attr = None

                            continue
                        else:
                            if cur_metadata_attr is not None:
                                # this line must be a continuation of the metadata tag
                                metadata[cur_metadata_attr].append(line.strip())
            else:
                # First check if the line is a new metadata attribute tag
                if line.strip() in metadata_attr_tags:
                    cur_metadata_attr = line.strip().rstrip(":").lower()
                else:
                    # next check if the line starts with a metadata attribute tag
                    for attr in metadata_attr_tags:
                        if line.startswith(attr):
                            cur_metadata_attr = attr.strip().rstrip(":").lower()

                            if not line.strip().endswith(attr):
                                # if line doesn't end with the attribute tag, there are values
                                # on the line and we assume it's a one line tag
                                metadata[cur_metadata_attr].append(line.strip().split(attr)[1].strip())
                                cur_metadata_attr = None

                            break
                    if not line.startswith(attr):
                        # this line must be a continuation of the metadata tag
                        metadata[cur_metadata_attr].append(line.strip())

        if METADATA_CONFIG["start_tag"] == line.strip():
            is_metadata_block = True

    f.close()

    return metadata


def main():
    print PROJECT_DIR
    filename = os.path.join(PROJECT_DIR, "tests", "test_data", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1log.txt")
    # filename =""
    extract_metadata(filename)

if __name__ == "__main__":
    main()