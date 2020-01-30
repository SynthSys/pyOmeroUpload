#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from collections import namedtuple
from metadata_parser import MetadataParser
import abc

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

LOG_METADATA_CONFIG = {
    "start_tag": "Experiment details:",
    "attribute_tags": ["Aim:", "Strain:", "Comments:", "Brightfield:",
                               "DIC:", "GFP:", "GFPFast:", "cy5:", "Omero project:",
                               "Omero tags:", "Experiment started at:"],
    "end_tag": "------Time point_1------"
    #"end_tag": "PFS is locked"
}

ACQ_METADATA_CONFIG = {
    "start_tag": "Experiment details:",
    "header_tags": ["Microscope name is:", "Acquisition settings are saved in:",
                                "Experiment details:", "Omero project:", "Omero tags:",
                                "Experiment started at:"],
    "parse_regex_tags": [""],
    "end_tag": "Experiment started at:"
}

class LogMetadataParser(MetadataParser):

    def get_str_array_val(self, metadata_val):
        str_val = ''

        if isinstance(metadata_val, list):
            str_val = metadata_val[0]
        elif isinstance(metadata_val, str):
            str_val = metadata_val

        return str_val


    def create_log_metadata_obj(self, metadata):
        log_metadata = namedtuple('LogMetadata', [], verbose=False)

        log_metadata.aim = self.get_str_array_val(metadata['aim'])
        log_metadata.project = self.get_str_array_val(metadata['omero project'])
        log_metadata.exp_start_date = self.get_str_array_val(metadata['experiment started at'])

        if len(metadata['strain']) > 0:
            log_metadata.strain = metadata['strain'][0]

        log_metadata.comments = metadata['comments']

        if len(metadata['brightfield']) > 0:
            log_metadata.brightfield = metadata['brightfield']

        log_metadata.dic = metadata['dic']
        log_metadata.gfp = metadata['gfp']
        log_metadata.cy5 = metadata['cy5']
        log_metadata.gfpfast = metadata['gfpfast']

        # attributes used directly in OMERO
        log_metadata.tags = metadata['omero tags']

        kvp_list = build_kvps('Brightfield', log_metadata.brightfield)
        kvp_list.extend(build_kvps('DIC', log_metadata.dic))
        kvp_list.extend(build_kvps('GFP', log_metadata.gfp))
        kvp_list.extend(build_kvps('GFPFast', log_metadata.gfpfast))
        kvp_list.extend(build_kvps('CY5', log_metadata.cy5))

        kvp_list.append(['Strain', log_metadata.strain])
        kvp_list.append(['Project', log_metadata.project])
        kvp_list.append(['Experiment Start Date', log_metadata.exp_start_date])
        kvp_list.append(['Number of pumps', str(acq_metadata.npumps)])

        log_metadata.kvps_list = kvp_list

        return log_metadata


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
    def extract_metadata(self, filename):
        # init the metadata attributes dict
        metadata = dict()

        # add each attribute tag as a key in the dict
        for attr_tag in LOG_METADATA_CONFIG["attribute_tags"]:
            metadata[attr_tag.rstrip(":").lower()] = list()

        metadata_attr_tags = LOG_METADATA_CONFIG["attribute_tags"]
        f = open(filename)  # This is a big file

        is_metadata_block = False
        cur_metadata_attr = None

        # read each line in the file
        for line in f:

            # exit reading the file if we have reached the end of metadata block
            if line.startswith(LOG_METADATA_CONFIG["end_tag"]):
                is_metadata_block = False
                break

            if line.startswith('PFS is locked'):
                continue

            if is_metadata_block:
                # if we're in the metadata block, check for a current attribute tag
                if cur_metadata_attr is None:
                    # First check if the line is a new metadata attribute tag
                    if line.strip() in metadata_attr_tags:
                        cur_metadata_attr = line.strip().rstrip(":").lower()
                    else:
                        # next check if the line starts with a metadata attribute tag
                        for attr in metadata_attr_tags:
                            if line.startswith(attr) or attr in line:
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
                            if line.startswith(attr) or attr in line:
                                cur_metadata_attr = attr.strip().rstrip(":").lower()

                                if not line.strip().endswith(attr):
                                    # if line doesn't end with the attribute tag, there are values
                                    # on the line and we assume it's a one line tag
                                    metadata[cur_metadata_attr].append(line.strip().split(attr)[1].strip())
                                    cur_metadata_attr = None

                                break
                        if not line.startswith(attr) and attr not in line:
                            # this line must be a continuation of the metadata tag
                            metadata[cur_metadata_attr].append(line.strip())

            if LOG_METADATA_CONFIG["start_tag"] == line.strip():
                is_metadata_block = True

        f.close()

        print metadata
        log_metadata = self.create_log_metadata_obj(metadata)

        return log_metadata


def main():
    print PROJECT_DIR
    '''
    filename = os.path.join(PROJECT_DIR, "tests", "test_data", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1log.txt")

    filename = os.path.join(PROJECT_DIR,  "tests", "test_data", "sample_logfiles",
                              "dataset_12655", "20171205_vph1hxt1log.txt")
    '''
    filename =  os.path.join(PROJECT_DIR, '..', 'Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00', 'Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1log.txt')

    # filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles", "dataset_846", "lowglc_screen_hog1_gln3_mig1_msn2_yap1log.txt")

    #filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
      #                        "dataset_8939", "sga_glc0_1_Mig1Nhp_Maf1Nhp_Msn2Maf1_Mig1Mig1_Msn2Dot6log.txt")

    #filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
      #                        "dataset_13606", "Hxt4GFP_hxt1log.txt")

    #filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
    #                         "dataset_14507", "Batgirl_Morph_OldCamera_Myo1_Lte1_Bud3_Htb2_Hog1log.txt")

    # metadata = extract_log_metadata(filename)
    metadata_parser = LogMetadataParser()
    metadata = metadata_parser.extract_metadata(filename)
    '''log_metadata = namedtuple('LogMetadata', [], verbose=False)
    log_metadata.aim = metadata['aim'][0]

    if len(metadata['strain']) > 0:
        log_metadata.strain = metadata['strain'][0]

    log_metadata.comments = metadata['comments']

    if len(metadata['brightfield']) > 0:
        log_metadata.brightfield = metadata['brightfield']

    log_metadata.dic = metadata['dic']
    log_metadata.gfp = metadata['gfp']
    log_metadata.project = metadata['omero project']
    log_metadata.tags = metadata['omero tags']
    log_metadata.exp_start_date = metadata['experiment started at']

    print log_metadata'''
    print metadata.tags
    print metadata.strain
    print metadata.dic
    print metadata.gfpfast


if __name__ == "__main__":
    main()