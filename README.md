# pyOmeroUpload
Project for uploading local file structure into OMERO database

# Building the Distribution
When building a distribution for release through BioConda, the Python setuptools are used. From the top directory, run either `python setup.py sdist` for a source distribution or `python setup.py bdist` for a binary distribution. If you receive an error relating to tag names and SCM, this has been traced to the .git/packed-refs file, which contains references to the Git branches and tags in the repository. For some reason, the PyScaffold/setuptools break when tags are present in this file. Therefore, lines in the packed-refs file that related to tags must be commented-out to allow the distribution to build.

## Testing the Distribution
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
