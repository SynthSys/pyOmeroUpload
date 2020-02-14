#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import namedtuple
import os
import yaml
from omero_data_transfer.omero_data_broker import OMERODataBroker
from omero_data_transfer.omero_data_broker import OMERODataType
from scipy import io as sio
from omero import model, rtypes, sys
import numpy as np
import re
from datetime import datetime as dt


OmeroCache = namedtuple('OmeroCache', 'Datasets Tags Projects')

DATE_FORMAT_1 = "%d-%b-%Y"
DATE_FORMAT_2 = "%Y_%b_%d"
DATE_TAG_FORMAT = "%Y-%m-%d"

PROJECT_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")


class OmeroCacheManager():
    # OmeroCode/@OmeroDatabase/createDbInfo.m
    def create_db_info(self, data_broker):
        print 'here'
        data_broker.open_omero_session()

        projects = data_broker.retrieve_objects(OMERODataType.project, None,
                                                None)

        for project in projects:
            print project

        data_broker.close_omero_session()

    def save_matlab_object(self):
        annotations = dict()
        annotations['positions'] = dict()
        annotations['positions']['pos001'] = 25
        annotations['positions']['pos002'] = 50
        annotations['pumps'] = [0.25, 2]
        annotations['channels'] = ['GFP', 'DIC', 'Brightfield']

        dir_path = os.path.join(PROJECT_DIR, "tests", "test_data", "matlab")
        sio.savemat(os.path.join(dir_path, 'annotations.mat'), annotations)

    def load_matlab_object(self):
        dir_path = os.path.join(PROJECT_DIR, "tests", "test_data", "matlab")
        dbTable4287 = sio.loadmat(os.path.join(dir_path, 'dbTable4287.mat'))

        # print dbTable4287

        dbData = sio.loadmat(os.path.join(dir_path, 'dbData.mat'))

        print dbData

    # essentially this function is translated from `OmeroDatabase.createDbInfo`
    def create_db_info(self, data_broker):
        print "balls"

    # essentially this function is translated from `OmeroDatabase.saveDbData`
    def save_db_data(self, data_broker):
        data_broker.open_omero_session()

        projects = data_broker.retrieve_objects(OMERODataType.project, [],
                                                None)

        db_data = dict()
        db_data_list = list()
        db_data["data"] = db_data_list

        for p_idx, project in enumerate(projects):
            print project
            # use container query to retrieve datasets/metadata
            ds_ids, ds_names, ds_descs = list(), list(), list()

            # this logic is adapted from getProjectDs.m
            for d_idx, dataset in enumerate(project.linkedDatasetList()):
                ds_tags = list()
                # [dsIds dsNames dsDescriptions dsTags] = project.getProjectDs(obj.Projects(p).name, obj.Projects(p).id);

                ds_id = dataset.getId().getValue()
                ds_ids.append(ds_id)

                db_data_list.insert(d_idx, list())
                db_data_list[d_idx].extend(np.array(list([ds_id]), dtype=np.uint16))

                # order is: dataset_id, project_name, dataset_name, dataset_tags
                db_data_list[d_idx].extend(np.array(list([project.getName().getValue()]),
                                                    dtype=np.unicode_))

                ds_names.append(dataset.getName().getValue())

                db_data_list[d_idx].extend(np.array(list([dataset.getName().getValue()]),
                                                dtype=np.unicode_))

                ds_desc = dataset.getDescription()

                if ds_desc != None:
                    ds_descs.append(ds_desc.getValue())
                else:
                    ds_descs.append("")

                # must use stateless (i.e. non-ICE API) to connect to different
                # OMERO server instances with different ICE versions
                # ds_annos = data_broker.load_object_annotations(
                #     OMERODataType.dataset, ds_id)
                params = sys.Parameters()
                params.map = {"did": rtypes.rlong(ds_id)}

                ds_annos = data_broker.find_objects_by_query(
                    data_broker.LINKED_ANNOS_BY_DS_QUERY, params).linkedAnnotationList()

                tag_str = ""
                tag_annos, unique_tags = list(), list()

                for anno in ds_annos:
                    if isinstance(anno, model.TagAnnotationI):
                        tag_annos.append(anno)
                        tag_name = anno.getTextValue().getValue()

                        if tag_name in unique_tags:
                            continue

                        unique_tags.append(tag_name)

                        if len(tag_str) == 0:
                            tag_str = tag_name.strip()
                        else:
                            tag_str = ",".join([tag_str, tag_name.strip()])

                ds_tags.append(tag_str)

                db_data_list[d_idx].extend(np.array(list([ds_tags]), dtype=np.unicode_))

                # add date tags
                date_tag_str = self.extract_date_tags(tag_annos)

                if len(date_tag_str) > 0:
                    db_data_list[d_idx].extend(np.array(list([date_tag_str]),
                                                        dtype=np.unicode_))

        data_broker.close_omero_session()

        # expected object is literally just an array of arrays, but would be nice
        # to save as a dictionary instead ...
        # dataset_dict = dict()
        # dataset_dict['dsIds'] = np.array(ds_ids, dtype=np.uint16)
        # dataset_dict['dsNames'] = np.array(ds_names, dtype=np.unicode_)
        # dataset_dict['dsDescriptions'] = np.array(ds_descs, dtype=np.unicode_)
        # dataset_dict['dsTags'] = np.array(ds_tags, dtype=np.unicode_)

        dir_path = os.path.join(PROJECT_DIR, "tests", "test_data", "matlab")

        sio.savemat(os.path.join(dir_path, 'dbData.mat'), db_data)

    # the function below is adapted from OmeroCode/addDateTags.m
    def extract_date_tags(self, tag_annotations):
        # tag_types = [tag_types.append(tag_anno.getDescription()
        #                                          .getValue()) for tag_anno in tag_annotations]

        tag_types = list(tag_anno.getDescription().getValue() for tag_anno in
                         tag_annotations if tag_anno.getDescription() is not None)
        print tag_types

        tag_strs = list(tag_anno.getTextValue().getValue() for tag_anno in
                        tag_annotations if tag_anno.getDescription() is not None)
        print tag_strs

        date = None
        date_str = ""
        has_date = False

        if "Date" in tag_types:
            date_str = tag_strs[tag_types.index("Date")]

            if len(date_str) > 0:
                date = self.parse_date_str(date_str, DATE_FORMAT_1)

                if date is not None:
                    date_str = dt.strftime(date, DATE_TAG_FORMAT)

                else:
                    date = self.parse_date_str(date_str, DATE_FORMAT_2)

                    if date is not None:
                        date_str = dt.strftime(date, DATE_TAG_FORMAT)

                has_date = True

        if has_date == False:
            # get the date from the id tag if it exists
            if "id" in tag_types:
                unique_id = tag_strs[tag_types.index("id")].strip()

                date = self.parse_date_str(unique_id, DATE_FORMAT_1)

                if date is not None:
                    date_str = dt.strftime(date, DATE_TAG_FORMAT)

                else:
                    date = self.parse_date_str(unique_id, DATE_FORMAT_2)

                    if date is not None:
                        date_str = dt.strftime(date, DATE_TAG_FORMAT)

            # if no ID tag, look for a matching date string
            if date is None:
                for tag_str in tag_strs:
                    date = self.parse_date_str(tag_str.strip(), DATE_FORMAT_1)

                    if date is not None:
                        date_str = dt.strftime(date, DATE_TAG_FORMAT)

            if date_str in tag_strs:
                # There is a date tag but it didn't turn up in tagTypes -
                # presumably not recorded in the description, so add it
                date_tag = tag_annotations[tag_strs.index(date_str)]
                    # .getTextValue().getValue().strip()
                date_tag.setDescription(rtypes.rstring('Date'))
                # should this service be read-only??!
                # uS = obj.Session.getUpdateService;
                # uS.saveAndReturnObject(dateTag);

        return date_str

    def parse_date_str(self, str, date_fmt):
        date = None

        try:
            date = dt.strptime(str.strip(), date_fmt)
        except Exception as error:
            print(error)

        return date


def main():
    CONFIG_FILE = os.path.join(PROJECT_DIR, 'config_test.yml')
    CONFIG = {}

    with open(CONFIG_FILE, 'r') as cfg:
        CONFIG = yaml.load(cfg, Loader=yaml.FullLoader)

    conn_settings = CONFIG['omero_conn']
    broker = OMERODataBroker(conn_settings)

    cache_manager = OmeroCacheManager()
    # broker.open_omero_session()

    dir_path = os.path.join(PROJECT_DIR, "tests", "test_data", "Morph_Batgirl_OldCamera_Htb2_Myo1_Hog1_Lte1_Vph1_00")

    # cache_manager.create_db_info(broker)
    # cache_manager.save_matlab_object()

    cache_manager.save_db_data(broker)
    cache_manager.load_matlab_object()

    # upload_metadata(broker, dir_path)

    broker.close_omero_session()


if __name__ == "__main__":
    main()