OMERO User Scripts
==================

Installation
------------

1. Change into the scripts location of your OMERO installation

        cd OMERO_DIST/lib/scripts

2. Create a new directory and download the script file

        mkdir omero_export && cd omero_export
        wget https://github.com/SynthSys/omero-toolkit/blob/master/pyOmeroUpload/src/omero_data_transfer/Export_to_other_omero.py

3. Update your list of installed scripts by examining the list of scripts
   in OMERO.insight or OMERO.web, or by running the following command

        path/to/bin/omero script list

Upgrading
---------

1. Change into the repository location created during installation

        cd OMERO_DIST/lib/scripts/omero_export

2. Update the repository to the latest version

        wget https://github.com/SynthSys/omero-toolkit/blob/master/pyOmeroUpload/src/omero_data_transfer/Export_to_other_omero.py

3. Update your list of installed scripts by examining the list of scripts
   in OMERO.insight or OMERO.web, or by running the following command

        path/to/bin/omero script list

Testing your script
-------------------

1. List the current scripts in the system

        path/to/bin/omero script list

2. List the parameters

        path/to/bin/omero script params SCRIPT_ID

3. Launch the script

        path/to/bin/omero script launch SCRIPT_ID

4. See the [developer documentation](https://www.openmicroscopy.org/site/support/omero4/developers/scripts/)
   for more information on testing and modifying your scripts.

Legal
-----

See [LICENSE](OMERO_EXPORT_LICENSE)


# About #
This section provides machine-readable information about your scripts.
It will be used to help generate a landing page and links for your work.
Please modify **all** values on **each** branch to describe your scripts.

###### Repository name ######
Base OMERO User Scripts repository

###### Minimum version ######
4.4

###### Maximum version ######
5.0

###### Owner(s) ######
The OME Team

###### Institution ######
Open Microscopy Environment

###### URL ######
http://openmicroscopy.org/info/scripts

###### Email ######
ome-devel@lists.openmicroscopy.org.uk

###### Description ######
Example script repository to be cloned, modified, and extended.
This text may be used on OME resources to explain your scripts.
