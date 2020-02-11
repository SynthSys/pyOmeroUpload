#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

# Instantiate the parser
parser = argparse.ArgumentParser(description='PyOmeroUpload Data Transfer Application')

# mandatory args
parser.add_argument('-d', '--data-path', dest='data_path',
    type=str, required=True, metavar='data-path',
    help="specifies the system file path to the data directory for uploading")

parser.add_argument('-n', '--dataset-name', dest='dataset_name',
    type=str, required=True, metavar='dataset-name',
    help="specifies the name of the destination dataset")

# optional args
parser.add_argument('-c', '--hypercube', dest='hypercube',
    type=bool, required=False, metavar='hypercube',
    help="commands the uploader to generate hypercube images")

parser.add_argument('-m', '--module-path', dest='module_path',
    type=str, required=False, metavar='module-path',
    help="specifies the system file path to the directory containing custom classes")

parser.add_argument('-p', '--parser-class', dest='parser_class',
    type=str, required=False, metavar='parser-class',
    help="specifies the name of the custom metadata parser class")

parser.add_argument('-i', '--image-processor-class', dest='image_processor_class',
    type=str, required=False, metavar='image-processor-class',
    help="specifies the name of the custom image processor class")

args = parser.parse_args()

if args.data_path:
    print args.data_path

if args.dataset_name:
    print args.dataset_name

if args.hypercube:
    print args.hypercube

if args.module_path:
    print args.module_path

if args.parser_class:
    print args.parser_class

if args.image_processor_class:
    print args.image_processor_class
