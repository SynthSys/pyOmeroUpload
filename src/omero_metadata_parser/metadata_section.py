#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import absolute_import
from six.moves import zip
__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

from collections import OrderedDict
from builtins import property as _property, tuple as _tuple
from operator import itemgetter as _itemgetter

class MetadataSection(tuple):
    'MetadataSection()'

    __slots__ = ()

    _fields = ('id', 'label', 'regex', 'data_type', 'rx')

    def __new__(_cls):
        'Create new instance of Point(x, y)'
        return _tuple.__new__(_cls)

    def __repr__(self):
        'Return a nicely formatted representation string'
        return 'MetadataSection(id=%r, label=%r, regex=%r, data_type=%r,' \
               'rx=%r)' % self

    def _asdict(self):
        'Return a new OrderedDict which maps field names to their values'
        return OrderedDict(list(zip(self._fields, self)))

    def __getnewargs__(self):
        'Return self as a plain tuple.  Used by copy and pickle.'
        return tuple(self)

    __dict__ = _property(_asdict)

    def __getstate__(self):
        'Exclude the OrderedDict from pickling'
        pass

    id = _property(_itemgetter(0), doc='Alias for field id')

    label = _property(_itemgetter(1), doc='Alias for field label')