#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

import abc

class ImageProcessor(object):
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod

    def process_images(self, omero_session, file_path, dataset):
        raise NotImplementedError('users must define process_images to use this base class')