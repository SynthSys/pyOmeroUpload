#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
import six
__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

import abc

class ImageProcessor(six.with_metaclass(abc.ABCMeta, object)):
    @abc.abstractmethod

    def process_images(self, omero_session, file_path, dataset):
        raise NotImplementedError('users must define process_images to use this base class')