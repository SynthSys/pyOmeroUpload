#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
import six
__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

import os
import json
import re
import datetime as dt
import ast
import pandas as pd
from collections import namedtuple
from omero_metadata_parser.metadata_parser import MetadataParser

try:
    from types import SimpleNamespace as Namespace
except ImportError:
    # Python 2.x fallback
    from argparse import Namespace

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")

# stores the data extracted from the metadata file
metadata_obj, tag_descriptions = dict(), dict()

attribute_keys = dict()

TableSection = namedtuple('TableSection',
                          'metadata_obj cur_metadata_attr is_table_section cur_table_section')

MetadataSection = namedtuple('MetadataSection',
                                         'cur_metadata_attr is_dict_section is_table_section '
                                         'cur_metadata_dict cur_key_label_regex')

AttributeKVP = namedtuple('AttributeKVP',
                                         'metadata_obj cur_dict_key cur_metadata_vals')

class TemplateMetadataParser(MetadataParser):

    def create_acq_metadata_obj(self, metadata):
        acq_metadata = namedtuple('AcqMetadata', [])

        acq_metadata.channels = self.get_str_array_val(metadata['channels'])
        acq_metadata.zsections = self.get_str_array_val(metadata['zsections'])

        acq_metadata.times = self.get_str_array_val(metadata['time_settings'])
        acq_metadata.positions = self.get_str_array_val(metadata['positions'])
        # acq_metadata.npumps = metadata['npumps']
        acq_metadata.pump_init = self.get_str_array_val(metadata['pumpstart'])
        acq_metadata.switch_params = metadata['switch_params']

        return acq_metadata


    def create_log_metadata_obj(self, metadata):
        log_metadata = namedtuple('LogMetadata', [])

        metadata['timepoint'] = list()

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
        log_metadata.tags = metadata['omero tags']

        return log_metadata


    # Extract values from nested dictionary by key
    def find_by_key(self, data, target):
        for key, value in six.iteritems(data):
            if isinstance(value, dict):
                for ele in self.find_by_key(value, target):
                    yield ele
            elif key == target:
                yield value


    # retrieve a dictionary in the config based on a key it contains
    def get_dict_by_child_key(self, data, sibling_key):
        for key, value in six.iteritems(data):
            if isinstance(value, dict):
                if sibling_key in list(value.keys()):
                    yield {key:value}
                    # yield value
                else:
                    for ele in self.find_by_key(value, sibling_key):
                        yield ele


    # def generate_metadata_sections()
    def build_regex_patterns(self, data):
        global metadata_obj
        metadata_sections = self.get_dict_by_child_key(metadata_obj, "regex")

        for metadata_section in list(metadata_sections):
            # print metadata_section

            # should only be one value dictionary, but just in case...
            for value_dict in metadata_section.values():
                value_dict["rx"] = re.compile(value_dict["regex"])

        # print metadata_obj


    def handle_table_metadata(self, s_line, metadata_obj, table_section, attribute):
        # handles table metadata and returns whether we are still processing a table
        is_table = False

        cur_table_section = table_section
        cur_metadata_attr = attribute

        # we must assume the table rows can be split like CSVs, with comma delimiters
        if ',' in s_line:
            if cur_table_section is None:
                # must be the columns header line to init the dataframe
                cols = s_line.split(',')
                cur_table_section = pd.DataFrame(columns=cols)
                is_table = True
            else:
                # must be a data row, but check that the number of cols match
                row_vals = s_line.split(',')
                if len(row_vals) == cur_table_section.shape[1]:
                    cur_table_section.loc[len(cur_table_section)] = row_vals
                    is_table = True
                else:
                    # exit logic, table row lines are exhausted
                    metadata_obj[cur_metadata_attr] = cur_table_section

                    # clear state
                    cur_table_section = None
                    is_table = False
                    cur_metadata_attr = None
        else:
            # exit logic, table row lines are exhausted
            metadata_obj[cur_metadata_attr] = table_section

            # clear state
            cur_table_section = None
            is_table = False
            cur_metadata_attr = None

        table_section = TableSection(metadata_obj, cur_metadata_attr, is_table,
                                        cur_table_section)
        return table_section


    def handle_metadata_section(self, metadata_section, dict_section,
                                value_dict, attribute, match):
        cur_metadata_list, cur_metadata_dict = None, None
        is_list_section, is_dict_section, is_table_section = False, False, False
        cur_metadata_attr = None
        cur_value_regex, cur_key_label_regex = None, None

        # if is_dict_section:
        if dict_section != None:
            metadata_obj[attribute] = dict_section
            # cur_metadata_dict = None
            # is_dict_section = False

            # append the matching value into the metadata data stuct
        if "data_type" in list(value_dict.keys()):
            if value_dict["data_type"] == "LIST":
                is_list_section = True
                cur_metadata_list = list()
                cur_key_label_regex = None

                value_dict["values"] = cur_metadata_list

                if "value_regex" in list(value_dict.keys()):
                    cur_key_label_regex = value_dict["value_regex"]

                # this will take several iterations, so set the current attr state
                if "label" in list(value_dict.keys()):
                    cur_metadata_attr = value_dict["label"]
                else:
                    cur_metadata_attr = list(metadata_section.keys())[0]

                if "values" in list(value_dict.keys()):
                    values = value_dict["values"]
                    values.append(match.string)
                else:
                    value_dict["values"] = list()
                    value_dict["values"].append(match.string)
            elif value_dict["data_type"] == "TABLE":
                is_table_section = True
                # this will take several iterations, so set the current attr state
                if "label" in list(value_dict.keys()):
                    cur_metadata_attr = value_dict["label"]
                else:
                    cur_metadata_attr = list(metadata_section.keys())[0]
            elif value_dict["data_type"] == "DICT":
                is_dict_section = True
                cur_metadata_dict = dict()
                cur_key_label_regex = None

                # cur_dict_section = metadata_section
                value_dict["values"] = cur_metadata_dict

                if "key_label_regex" in list(value_dict.keys()):
                    cur_key_label_regex = value_dict["key_label_regex"]

                # this will take several iterations, so set the current attr state
                if "label" in list(value_dict.keys()):
                    cur_metadata_attr = value_dict["label"]
                else:
                    cur_metadata_attr = list(metadata_section.keys())[0]

        metadata_section = MetadataSection(cur_metadata_attr, is_dict_section,
                                        is_table_section, cur_metadata_dict, cur_key_label_regex)
        return metadata_section


    def handle_attribute_kvp(self, s_line, metadata_obj, attribute, label_regex,
                            attribute_kvps, is_dict_section, metadata_dict, dict_section,
                            dict_key, metadata_vals):
        # check the line doesn't contain a date, e.g. '05-Dec-2017 09:45:37'
        date_regex = '\d{2}-\w{3}-\d{4} \d{2}:\d{2}:\d{2}'
        date_format = '%d-%b-%Y %H:%M:%S'
        date_match = re.search(date_regex, s_line)

        safe_text = s_line
        line_date = None

        cur_dict_key = dict_key

        cur_metadata_vals = metadata_vals

        if date_match:
            line_date = dt.datetime.strptime(date_match.group(), date_format)
            # replace the datetime value with a placeholder to prevent splitting on
            # colons
            safe_text = re.sub(date_regex, '${datetime}', s_line)

        if ':' in safe_text:
            parts = safe_text.split(':')

            for idx, part in enumerate(parts):
                # assume if it's an even number of parts, then it's KVP sequence
                if (len(parts) % 2 == 0 and idx % 2 == 0) or (len(parts) % 2 != 0 and idx == 0):
                    # we're in an an attribute key index, e.g. 0, 2, 4
                    key_match = False

                    # # # 4. Process text that corresponds to a dict format, i.e. KVP # # #
                    if is_dict_section:
                        # assume it's a dict containing list values for KVPs
                        if label_regex is None:
                            cur_dict_key = part
                            metadata_dict[part] = list()
                        else:
                            for key, value in six.iteritems(label_regex):
                                rx = re.compile(value)
                                match = rx.search(safe_text)

                                if match:
                                    cur_dict_key = key
                                    metadata_dict[key] = list()
                                    key_match = True

                    if key_match == False:
                        # # # 5. Standard KVP text, unnamed attribute # # #
                        # assume we have started a new attribute
                        if attribute != None:
                            attribute_kvps.append((attribute, cur_metadata_vals))

                        cur_metadata_attr = part

                        if is_dict_section:
                            metadata_obj[cur_metadata_attr] = dict_section
                            metadata_dict = None
                            is_dict_section = False

                    cur_metadata_vals = list()
                else:
                    # we're in an attribute value index, e.g. 1, 3, 5
                    if '${datetime}' in part:
                        part = re.sub('\${datetime}', dt.date.strftime(line_date,
                                                                    date_format), part)

                    if is_dict_section:
                        if len(part.rstrip()) > 0:
                            metadata_dict[cur_dict_key].append(part)
                    else:
                        cur_metadata_vals.append(part)
        else:
            if is_dict_section:
                metadata_dict[cur_dict_key].append(safe_text)
            else:
                cur_metadata_vals.append(safe_text)

        attribute_kvp = AttributeKVP(metadata_obj, cur_dict_key, cur_metadata_vals)
        return attribute_kvp


    def parse_metadata(self, schema, filename):
        f = open(filename)  # This is a big file

        cur_metadata_attr = None
        cur_metadata_vals = list()

        # list of lists containing KVPs
        attribute_kvps = list()

        global attribute_keys, metadata_obj

        self.build_regex_patterns(metadata_obj)

        # indicates if we are currently extracting a metadata table
        is_table_section, is_dict_section = False, False
        cur_table_section, cur_metadata_dict, cur_dict_section = None, None, None
        cur_dict_key, cur_key_label_regex = None, None

        # read each line in the file
        for line in f:
            s_line = line.rstrip()
            is_metadata_section = False

            # # # 1. Process text that corresponds to a table format, i.e. CSV # # #
            if is_table_section:
                table_section = self.handle_table_metadata(s_line, metadata_obj,
                                                    cur_table_section, cur_metadata_attr)
                cur_metadata_attr = table_section.cur_metadata_attr
                is_table_section = table_section.is_table_section
                cur_table_section = table_section.cur_table_section
                metadata_obj = table_section.metadata_obj

                if is_table_section:
                    # no need to continue executing remainder of logic for the table line
                    continue

            # # # 2. Process named metadata KVP attributes from JSON config # # #
            # iterate over metadata sections and look for line matches with regex
            metadata_sections = self.get_dict_by_child_key(metadata_obj, "rx")
            for metadata_section in list(metadata_sections):

                for value_dict in metadata_section.values():
                    match = value_dict["rx"].search(s_line)

                    if match:
                        metadata_section = self.handle_metadata_section(metadata_section,
                                                                cur_dict_section, value_dict,
                                                                cur_metadata_attr, match)
                        # populate state
                        cur_metadata_attr = metadata_section.cur_metadata_attr
                        is_table_section = metadata_section.is_table_section
                        is_dict_section = metadata_section.is_dict_section
                        cur_metadata_dict = metadata_section.cur_metadata_dict
                        cur_key_label_regex = metadata_section.cur_key_label_regex

                        # if we have a match, this line is not a generic KVP so we do not
                        # want to continue processing with string delimiter logic
                        is_metadata_section = True

            # no need to continue parsing for colon-separated KVPs
            if is_metadata_section == True:
                continue

            # # # 3. Process standard Key-value pair, colon-delimited text # # #
            attribute_kvp = self.handle_attribute_kvp(s_line, metadata_obj, cur_metadata_attr,
                                                    cur_key_label_regex, attribute_kvps,
                                                    is_dict_section, cur_metadata_dict,
                                                    cur_dict_section, cur_dict_key, cur_metadata_vals)

            cur_dict_key = attribute_kvp.cur_dict_key
            cur_metadata_attr = attribute_kvp.cur_metadata_vals
            metadata_obj = attribute_kvp.metadata_obj

        metadata_obj["attributes"] = attribute_kvps
        # print metadata_obj


    def get_tag_descriptions(self):
        global tag_descriptions
        return tag_descriptions


    def extract_acq_metadata(self, filename):
        # load JSON parser config
        json_fp = os.path.join(PROJECT_DIR, "resources", "acq_metadata_config.json")

        with open(json_fp, 'r') as json_file:
            # data = json.loads(json_file.read(), object_hook=lambda d: Namespace(**d))
            # print data
            schema = json.load(json_file, object_hook=lambda d: Namespace(**d))
            # print schema

        global metadata_obj
        # convert JSON object into a standard dictionary
        metadata_obj = ast.literal_eval(json.dumps(schema, default=lambda o: getattr(o, '__dict__', str(o))))["sections"]

        # print schema.time_point.label
        # print schema.time_point.regex

        # x = json2obj(json.dumps(data, encoding="UTF-8"))
        # print x

        self.parse_metadata(schema, filename)

        acq_metadata = self.create_acq_metadata_obj(metadata_obj)
        return acq_metadata


    def extract_log_metadata(self, filename):
        # load JSON parser config
        json_fp = os.path.join(PROJECT_DIR, "resources", "log_metadata_config.json")

        with open(json_fp, 'r') as json_file:
            # data = json.loads(json_file.read(), object_hook=lambda d: Namespace(**d))
            # print data
            schema = json.load(json_file, object_hook=lambda d: Namespace(**d))
            # print schema

        global metadata_obj, tag_descriptions
        # convert JSON object into a standard dictionary
        metadata_obj = ast.literal_eval(json.dumps(schema, default=lambda o: getattr(o, '__dict__', str(o))))["sections"]
        tag_descriptions = ast.literal_eval(json.dumps(schema, default=lambda o: getattr(o, '__dict__', str(o))))["tags"]
        # print metadata_obj

        # print schema.time_point.label
        # print schema.time_point.regex

        # x = json2obj(json.dumps(data, encoding="UTF-8"))
        # print x

        self.parse_metadata(schema, filename)

        log_metadata = self.create_log_metadata_obj(metadata_obj)
        return log_metadata


    def extract_metadata(self, filename):
        lc_filename = filename.lower()
        json_fp, metadata = None, None
        is_log, is_acq = False, False

        global metadata_obj, tag_descriptions

        if(lc_filename.rfind('log') == (lc_filename.rfind('.')-3)):
            is_log = True
            json_fp = os.path.join(PROJECT_DIR, "resources", "log_metadata_config.json")
        elif(lc_filename.rfind('acq') == (lc_filename.rfind('.')-3)):
            is_acq = True
            json_fp = os.path.join(PROJECT_DIR, "resources", "acq_metadata_config.json")

        with open(json_fp, 'r') as json_file:
            # load JSON parser config
            schema = json.load(json_file, object_hook=lambda d: Namespace(**d))

        # convert JSON object into a standard dictionary
        metadata_obj = ast.literal_eval(json.dumps(schema, default=lambda o: getattr(o, '__dict__', str(o))))["sections"]
        tag_descriptions = ast.literal_eval(json.dumps(schema, default=lambda o: getattr(o, '__dict__', str(o))))["tags"]

        if(is_log == True):
            metadata = self.extract_log_metadata(filename)

        if(is_acq == True):
            metadata = self.extract_acq_metadata(filename)

        return metadata


def main():
    # print PROJECT_DIR
    # filename = os.path.join(PROJECT_DIR, "tests", "test_data", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1log.txt")

    # test log file parsing ------------------------------------------
    # filename = os.path.join(PROJECT_DIR,  "tests", "test_data", "sample_logfiles",
    #                           "dataset_12655", "20171205_vph1hxt1log.txt")

    # filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles", "dataset_846", "lowglc_screen_hog1_gln3_mig1_msn2_yap1log.txt")

    #filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
      #                        "dataset_8939", "sga_glc0_1_Mig1Nhp_Maf1Nhp_Msn2Maf1_Mig1Mig1_Msn2Dot6log.txt")

    #filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
      #                        "dataset_13606", "Hxt4GFP_hxt1log.txt")

    # filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
    #                         "dataset_14507", "Batgirl_Morph_OldCamera_Myo1_Lte1_Bud3_Htb2_Hog1log.txt")



    # test acq file parsing ------------------------------------------
    # filename = os.path.join(PROJECT_DIR,  "tests", "test_data", "sample_logfiles",
    #                           "dataset_12655", "20171205_vph1hxt1Acq.txt")

    # filename = os.path.join(PROJECT_DIR, "tests", "test_data",
    #                           "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00",
    #                           "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1Acq.txt")
    #
    # filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
    #                          "dataset_846", "lowglc_screen_hog1_gln3_mig1_msn2_yap1Acq.txt")

    #filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
     #                         "dataset_8939", "sga_glc0_1_Mig1Nhp_Maf1Nhp_Msn2Maf1_Mig1Mig1_Msn2Dot6Acq.txt")

    #filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
     #                         "dataset_12655", "20171205_vph1hxt1Acq.txt")

    #filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
    #                          "dataset_13606", "Hxt4GFP_hxt1Acq.txt")

    # filename = os.path.join(PROJECT_DIR, "tests", "test_data", "sample_logfiles",
    #                       "dataset_14507", "Batgirl_Morph_OldCamera_Myo1_Lte1_Bud3_Htb2_Hog1Acq.txt")

    dir_path = os.path.join(PROJECT_DIR, "..", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")

    exp_file, acq_metadata = None, None

    metadata_parser = TemplateMetadataParser()

    # retrieve the metadata from the log file
    for exp_file in os.listdir(dir_path):
        if str.lower(exp_file).endswith("acq.txt"):  # the log file containing metadata

            acq_metadata = metadata_parser.extract_metadata(os.path.join(dir_path, exp_file))

            break

    exp_file, log_metadata = None, None

    # retrieve the metadata from the log file
    for exp_file in os.listdir(dir_path):
        if str.lower(exp_file).endswith("log.txt"):  # the log file containing metadata

            # log_metadata = metadata_parser.extract_metadata(os.path.join(dir_path, exp_file))

            break


if __name__ == '__main__':
    main()