import abc

class MetadataParser(object):
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod

    def extract_metadata(self, filename):
        raise NotImplementedError('users must define extract_metadata to use this base class')