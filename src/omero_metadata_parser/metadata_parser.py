#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
import six
from six.moves import range
__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

import abc

class MetadataParser(six.with_metaclass(abc.ABCMeta, object)):
    @abc.abstractmethod
    def extract_metadata(self, filename):
        raise NotImplementedError('users must define extract_metadata to use this base class')

    @classmethod
    def get_str_array_val(cls, metadata_val):
        str_val = ''

        if isinstance(metadata_val, list):
            str_val = metadata_val[0]
        elif isinstance(metadata_val, str):
            str_val = metadata_val

        return str_val

    # construct the KVPs strings from the main key (e.g. 'Brightfield') and the
    # metadata values, so all KVPs are returned as an array of pairs arrays
    @classmethod
    def build_kvps(cls, kvp_key, metadata_kvps):
        kvp_list = []
        cur_kvp_key = kvp_key
        cur_kvp_val = ''

        for idx, kvp_str in enumerate(metadata_kvps):

            # check if there's a colon; if not, assume it's the value of the current key
            # if it contains a colon, it must be a KVP
            kvps = kvp_str.split(':')

            if len(kvps) == 1:
                if len(cur_kvp_val.strip()) == 0:
                    cur_kvp_val = kvps[0]
                else:
                    cur_kvp_val = ';'.join([cur_kvp_val, cls.get_str_array_val(kvp_str.strip())])
            elif len(kvps) > 1:
                # append the current KVP into the list
                kvp_list.append([cur_kvp_key, cls.get_str_array_val(cur_kvp_val)])

                for i in range(0, int(len(kvps)/2)):
                    cur_kvp_key = kvps[0+i]
                    cur_kvp_val = kvps[1+i]

                    kvp_list.append([cur_kvp_key, cls.get_str_array_val(cur_kvp_val)])

        # if len(kvp_list) == 0:
        #     kvp_list = None

        return kvp_list