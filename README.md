
# pyOmeroUpload
A project providing Python code for uploading data and metadata from a local file structure into an OMERO server instance.

## Building the Distribution
When building a distribution for release through BioConda, the Python setuptools are used. From the top directory, run either `python setup.py sdist` for a source distribution or `python setup.py bdist` for a binary distribution. If you receive an error relating to tag names and SCM, this has been traced to the .git/packed-refs file, which contains references to the Git branches and tags in the repository. For some reason, the PyScaffold/setuptools break when tags are present in this file. Therefore, lines in the packed-refs file that related to tags must be commented-out to allow the distribution to build.

### Testing the Distribution
Before submitting the pull request to bioconda, it's a good idea to test that the new package is built properly. In a Linux system with Docker installed, run the following commands to test locally:

```
docker pull bioconda/bioconda-utils-build-env:latest
docker run -td bioconda/bioconda-utils-build-env /bin/bash
docker exec -it distracted_bartik /bin/bash
git clone https://github.com/SynthSys/bioconda-recipes
cd bioconda-recipes
git checkout pyOmeroUpload-recipe
bioconda-utils build --packages pyomero-upload --force
```
This will force the package to rebuild regardless of whether there are any uncommitted changes. You can edit the `bioconda-recipes/recipes/pyomero-upload` `meta.yaml` and `build.sh` files as required and rerun the command. The built package can be inspected in `/opt/conda/pkgs`.

## Installation
On a Unix-like system, the Conda package can be installed using standard installation commands:
```
$ conda create --name omero_upload python=2.7
$ conda activate omero_upload
$ conda install --channel conda-forge --channel bioconda pyomero-upload=5.4.10_1.3.0
```
For Windows users, Bioconda is not supported and therefore the OMEROConnect ([https://github.com/SynthSys/OMEROConnect](https://github.com/SynthSys/OMEROConnect)) Docker images must be used. The basic uploader package can be installed with the following command:
```
$ docker pull biordm/omero-connect:omero_uploader
```
Once installed, run the Docker container with this command:
```
$ docker run -t -d --name omero-uploader --entrypoint /bin/bash biordm/omero-connect:omero_uploader
```
The resulting uploader Docker container can then be accessed using the command:
```
$ docker exec -it omero-uploader /bin/bash
```
At this point, the Conda environment including the pyomero-upload package can be activated and the CLI can be invoked using the instructions below.

## Usage

### <a name="upload_cli">Uploading with the Upload CLI</a>
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
| -\-include-provenance-metadata | -v | If present, instructs the uploader to automatically include provenenance metadata. | N | True  |  
| -\-ignore-metadata | -x | If present, instructs the uploader to ignore metadata parsing and only upload images. | N | False  |  

The user specifies the target directory and, if desired, a custom module path containing an alternative metadata parser, and custom data transformation function with which to process collections of single images into _n_-dimensional images.
