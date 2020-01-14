# pyOmeroUpload
Project for uploading local file structure into OMERO database

# Building the Distribution
When building a distribution for release through BioConda, the Python setuptools are used. From the top directory, run either `python setup.py sdist` for a source distribution or `python setup.py bdist` for a binary distribution. If you receive an error relating to tag names and SCM, this has been traced to the .git/packed-refs file, which contains references to the Git branches and tags in the repository. For some reason, the PyScaffold/setuptools break when tags are present in this file. Therefore, lines in the packed-refs file that related to tags must be commented-out to allow the distribution to build.
