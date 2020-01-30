import os
import re
from collections import OrderedDict, namedtuple
from metadata_parser import MetadataParser
from extract_acq_metadata import AcqMetadataParser
from extract_log_metadata import LogMetadataParser
import glob
import abc

import pandas as pd

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")

# Load the input file to a variable
print PROJECT_DIR

class MetadataAggregator(MetadataParser):

    def extract_metadata(self, filename):
        dir_path = filename
        input_path = glob.glob(os.path.join(dir_path,'*[Aa]cq.txt'))
        print input_path

        # handle acquisition metadata file parsing
        # input_file = open(input_path[0])
        acq_parser = AcqMetadataParser()
        acq_metadata = acq_parser.extract_metadata(input_path[0])
        print acq_metadata

        input_path = glob.glob(os.path.join(dir_path,'*[Ll]og.txt'))
        print input_path

        # handle acquisition metadata file parsing
        log_parser = LogMetadataParser()
        log_metadata = log_parser.extract_metadata(input_path[0])
        print log_metadata

        merged_metadata = merge_object_properties(log_metadata, acq_metadata)

        return merged_metadata

    def merge_object_properties(object_to_merge_from, object_to_merge_to):
        """
        Used to copy properties from one object to another if there isn't a naming conflict;
        """
        for property in object_to_merge_from.__dict__:
            #Check to make sure it can't be called... ie a method.
            #Also make sure the object_to_merge_to doesn't have a property of the same name.
            if not callable(object_to_merge_from.__dict__[property]) and not hasattr(object_to_merge_to, property):
                setattr(object_to_merge_to, property, getattr(object_to_merge_from, property))

        return object_to_merge_to