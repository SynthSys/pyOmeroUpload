# pyOmeroUpload
A project providing Python code for uploading data and metadata from a local file structure into an OMERO server instance.

## Installation

## <a name="upload_cli">Uploading with the Upload CLI</a>
A very basic use case for uploading a set of test images as hypercubes and accompanying metadata, as provided in [https://github.com/SynthSys/omero_connect_demo](https://github.com/SynthSys/omero_connect_demo) using the default metadata parser is as follows (and you will be prompted for the password):
```
/opt/conda/bin/python -m pyomero_upload/upload_cli -d test_data -n test_upload -u user -s demo.openmicroscopy.org -y
```
The full set of options for the script are:
```
python -m pyomero_upload/upload_cli -d {DATA_DIRECTORY}  -n {DATASET_NAME} -u {USERNAME} -s {OMERO_SERVER_NAME} -y {USE_HYPERCUBE}  -m {CUSTOM_MODULE_PATH} -p {USE_CUSTOM_PARSER} -i {USE_CUSTOM_PROCESSOR}
```
The options are described in the table below.
| Parameter Name | Short Form | Description | Mandatory | Default |
|--|--|--|--|--|
| -\-config-file | -c | The absolute path to the file containing the standard configuration for connecting to a specified OMERO server instance  | N |  |  
| -\-username | -u | The username for the account on the target OMERO server | Y |  |  
| -\-server | -s | The server name of the target OMERO instance | Y |  |  
| -\-port | -o | The port of the target OMERO server instance | N | 4064 |  
| -\-data-path | -d | The absolute path to the data directory containing data and metadata to be uploaded | Y |  |  
| -\-dataset-name | -n | The name of the dataset to be uploaded to the OMERO server | Y |  |  
| -\-hypercube | -u | If present, performs conversion of the data in the data directory into multi-dimensional images for upload to OMERO as hypercubes. | N | False  |  
| -\-module-path | -m | The absolute path to the directory containing any custom classes required for metadata parsing or image processing | N |  |  
| -\-custom-metadata-parser | -p | If present, and if module-path is specified, use the class CustomMetadataParser provided in the module file custom_metadata_parser.py | N | omero_metadata_parser/aggregate_metadata.MetadataAggregator |  
| -\-custom-image-processor | -i | If present, and if module-path is specified, use the class CustomImageProcessor provided in the module file custom_image_processor.py | N | omero_data_transfer/default_image_processor.DefaultImageProcessor |  

The user specifies the target directory and, if desired, a custom module path containing an alternative metadata parser, and custom data transformation function with which to process collections of single images into _n_-dimensional images.
