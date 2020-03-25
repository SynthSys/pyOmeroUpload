#!/usr/bin/env python
# -*- coding: utf-8 -*-

__author__ = "Johnny Hay"
__copyright__ = "BioRDM"
__license__ = "mit"

from collections import OrderedDict
from builtins import property as _property, tuple as _tuple
from operator import itemgetter as _itemgetter


class LogMetadata(tuple):
    'LogMetadata()'

    __slots__ = ()

    _fields = ('aim', 'authors', 'strain', 'comments', 'brightfield', 'dic', 'gfp', 'gfpfast', 'cy5',
               'project', 'description', 'tags', 'exp_start_date', 'table_dict', 'kvp_list')

    def __new__(_cls):
        'Create new instance of Point(x, y)'
        return _tuple.__new__(_cls)

    def __repr__(self):
        'Return a nicely formatted representation string'
        return 'LogMetadata(aim=%r, strain=%r, comments=%r, brightfield=%r,' \
               'dic=%r, gfp=%r, project=%r, tags=%r, exp_start_date=%r)' % self

    def _asdict(self):
        'Return a new OrderedDict which maps field names to their values'
        return OrderedDict(zip(self._fields, self))

    def __getnewargs__(self):
        'Return self as a plain tuple.  Used by copy and pickle.'
        return tuple(self)

    __dict__ = _property(_asdict)

    def __getstate__(self):
        'Exclude the OrderedDict from pickling'
        pass

    aim = _property(_itemgetter(0), doc='Alias for field aim')

    strain = _property(_itemgetter(1), doc='Alias for field strain')