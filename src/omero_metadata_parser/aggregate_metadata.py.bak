#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

import os
from omero_metadata_parser.metadata_parser import MetadataParser
from omero_metadata_parser.extract_acq_metadata import AcqMetadataParser
from omero_metadata_parser.extract_log_metadata import LogMetadataParser
import glob


class MetadataAggregator(MetadataParser):

    def extract_metadata(self, filename):
        dir_path = filename
        input_path = glob.glob(os.path.join(dir_path,'*[Aa]cq.txt'))

        # handle acquisition metadata file parsing
        # input_file = open(input_path[0])
        acq_parser = AcqMetadataParser()
        acq_metadata = acq_parser.extract_metadata(input_path[0])

        input_path = glob.glob(os.path.join(dir_path,'*[Ll]og.txt'))

        # handle acquisition metadata file parsing
        log_parser = LogMetadataParser()
        log_metadata = log_parser.extract_metadata(input_path[0])

        merged_metadata = self.merge_object_properties(log_metadata, acq_metadata)

        return merged_metadata

    def merge_object_properties(self, object_to_merge_from, object_to_merge_to):
        """
        Used to copy properties from one object to another if there isn't a naming conflict;
        """
        for property in object_to_merge_from.__dict__:
            #Check to make sure it can't be called... ie a method.
            #Also make sure the object_to_merge_to doesn't have a property of the same name.
            if not callable(object_to_merge_from.__dict__[property]):
                if not hasattr(object_to_merge_to, property):
                    setattr(object_to_merge_to, property, getattr(object_to_merge_from, property))
                else:
                    from_attr = getattr(object_to_merge_from, property)
                    to_attr = getattr(object_to_merge_to, property)
                    if type(from_attr) is dict:
                        new_dict = from_attr.copy()
                        new_dict.update(to_attr)
                        setattr(object_to_merge_to, property, new_dict)
                    elif type(from_attr) is list:
                        new_list = from_attr + to_attr
                        setattr(object_to_merge_to, property, new_list)

        return object_to_merge_to