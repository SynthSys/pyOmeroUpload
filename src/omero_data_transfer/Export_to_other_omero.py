#!/usr/bin/env python
# -*- coding: utf-8 -*-

#
# Adapted from https://github.com/openmicroscopy/openmicroscopy/blob/develop/
# examples/Training/python/Scripting_Service_Example.py
#

# This script takes an Image ID, a username and a password as parameters from
# the scripting service.
from __future__ import absolute_import
from __future__ import print_function
import omero
from omero.rtypes import rlong, rstring, unwrap
import os
import omero.cli
import omero.scripts as scripts
import omero.util.script_utils as script_utils
from omero.gateway import BlitzGateway
import sys
from tempfile import NamedTemporaryFile
from contextlib import contextmanager
from omero.constants.namespaces import NSCREATED
import glob
import zipfile
import re
from six.moves import range

try:
    from PIL import Image  # see ticket:2597
except ImportError:
    import Image

# old_stdout = sys.stdout
# temp_file = NamedTemporaryFile(delete=False)
# sys.stdout = temp_file

# REMOTE_HOST = 'demo.openmicroscopy.org'
REMOTE_HOST = 'publicomero.bio.ed.ac.uk'
REMOTE_PORT = 4064
DEFAULT_PROJECT = 'Default project'


# keep track of log strings.
log_strings = []
pre_loaded_tags = []
include_annos = True

def compress(target, base):
    """
    Creates a ZIP recursively from a given base directory.

    @param target:      Name of the zip file we want to write E.g.
                        "folder.zip"
    @param base:        Name of folder that we want to zip up E.g. "folder"
    """
    zip_file = zipfile.ZipFile(target, 'w')
    try:
        files = os.path.join(base, "*")
        for name in glob.glob(files):
            zip_file.write(name, os.path.basename(name), zipfile.ZIP_DEFLATED)

    finally:
        zip_file.close()


def log(text):
    """
    Adds the text to a list of logs. Compiled into text file at the end.
    """
    # Handle unicode
    try:
        text = text.encode('utf8')
    except UnicodeEncodeError:
        pass
    log_strings.append(str(text))


# A couple of helper methods for capturing sys.stdout
#

# From stackoverflow https://stackoverflow.com/questions/4675728/redirect-stdout
# -to-a-file-in-python/22434262#22434262


def fileno(file_or_fd):
    fd = getattr(file_or_fd, 'fileno', lambda: file_or_fd)()
    if not isinstance(fd, int):
        raise ValueError("Expected a file (`.fileno()`) or a file descriptor")
    return fd


@contextmanager
def stdout_redirected(to=os.devnull, stdout=None):
    if stdout is None:
        stdout = sys.stdout

    stdout_fd = fileno(stdout)
    # copy stdout_fd before it is overwritten
    # NOTE: `copied`is inheritable on Windows when duplicating a standard stream
    with os.fdopen(os.dup(stdout_fd), 'wb') as copied:
        stdout.flush()  # flush library buffers that dup2 knows nothing about
        try:
            os.dup2(fileno(to), stdout_fd)  # $ exec >&to
        except ValueError:  # filename
            with open(to, 'wb') as to_file:
                os.dup2(to_file.fileno(), stdout_fd)  # $ exec > to
        try:
            yield stdout  # allow code to be run with the redirected stdout
        finally:
            # restore stdout to its previous value
            # NOTE: dup2 makes stdout_fd inheritable unconditionally
            stdout.flush()
            os.dup2(copied.fileno(), stdout_fd)  # $ exec >&copied


# determine whether the user has privileges to read the managed.data.dir
# configuration parameter
def check_omero_config():
    has_privs = False
    scripts_timeout = 3600000
    client = None

    try:
        client = omero.client()
        client.createSession()
        conn = omero.gateway.BlitzGateway(client_obj=client)
        conn.SERVICE_OPTS.setOmeroGroup(-1)
        scripts_timeout = int(client.sf.getConfigService().getConfigValue(
            "omero.scripts.timeout"))

        managed_dir = client.sf.getConfigService().getConfigValue(
            "omero.managed.dir")

        has_privs = True
    except Exception as e:
        print('Exception: %s' % e)
    finally:
        client.closeSession()

    return {
        "has_privs": has_privs,
        "scripts_timeout": scripts_timeout
    }

# End helper methods for capturing sys.stdout
def run_script():
    # Script definition
    # Script name, description and 2 parameters are defined here.
    # These parameters will be recognised by the Insight and web clients and
    # populated with the currently selected Image(s)
    # A username and password will be entered too.
    # this script takes Images or Datasets
    message = "Script to export a file to another omero server."
    data_types = [rstring('Dataset'), rstring('Image')]

    omero_config = check_omero_config()

    has_privs = omero_config["has_privs"]
    scripts_timeout = omero_config["scripts_timeout"]
    scripts_timeout = (scripts_timeout / 1000) / 60

    if has_privs == True:
        message = "\n\n".join([message, "You are running this script with"
                                      " administrator privileges and the omero.scripts.timeout"
                                      " configuration parameter is {}"
                                      " minutes.".format(scripts_timeout)])
    else:
        message = "\n\n".join([message, "Please note, you are running this script as a"
                                      " non-administrator so we are unable to provide an accurate"
                                      " value for the omero.scripts.timeout configuration"
                                      " parameter. The default of {} minutes is assumed,"
                                      " though you should check with your OMERO"
                                      " administrator.".format(scripts_timeout)])

    image_formats = [rstring('Hypercube'), rstring('TIFF')]

    client = scripts.client(
        "Export_to_other_omero.py",
        message,
        scripts.String(
            "Data_Type", optional=False, values=data_types, default="Image",
            description="The data you want to work with.", grouping="1.1"),
        scripts.List("IDs", optional=False, grouping="1.2",
                     description="List of Dataset IDs or Image IDs").ofType(
            rlong(0)),

        # Username
        scripts.String("Username", optional=False, grouping="2.1"),
        scripts.String("Password", optional=False, grouping="2.2"),
        scripts.String("Image_Format", optional=False, values=image_formats,
                       default="Hypercube", grouping="3.1"),
        scripts.Bool("Include_Annotations", grouping="3.2", default=True)
    )

    try:
        # we can now create our local Blitz Gateway by wrapping the client.
        local_conn = BlitzGateway(client_obj=client)
        script_params = client.getInputs(unwrap=True)

        message = copy_to_remote_omero(client, local_conn, script_params)
    finally:
        client.setOutput("Message: ", rstring(message))

    # Return some value(s).
    # Here, we return anything useful the script has produced.
    # NB: The Insight and web clients will display the "Message" output.
    # msg = "Script ran with Image ID: %s, Name: %s and \nUsername: %s"\
    #       % (image_id, image.getName(), username)
    # client.setOutput("Message", rstring(msg))
        client.closeSession()

        try:
            local_conn.close()
        except Exception as e:
            msg = "Error closing local connection: %s" % (e)


def copy_to_remote_omero(client, local_conn, script_params):
    # TODO could maybe refactor to remove client
    data_type = script_params["Data_Type"]
    username = script_params["Username"]
    password = script_params["Password"]
    image_format = script_params["Image_Format"]

    global include_annos
    include_annos = script_params["Include_Annotations"]
    # The managed_dir is where the local images are stored.
    # TODO could pass this in instead of client?
    # This directory requires administrator privileges to access; which is why the
    # script fails if it is run by a non admin-user
    managed_dir = None

    try:
        managed_dir = client.sf.getConfigService().getConfigValue(
            "omero.managed.dir")
    except:
        from os.path import expanduser
        managed_dir = os.path.join(expanduser("~"), 'omero_data', 'ManagedRepository')

    # # Get the images or datasets
    message = ""
    objects, log_message = script_utils.get_objects(local_conn, script_params)
    message += log_message
    if not objects:
        return message

    try:
        # Connect to remote omero
        c, cli, remote_conn = connect_to_remote(password, username)

        images = []
        if data_type == 'Dataset':
            # TODO handle multiple datasets
            for ds in objects:
                dataset_name = ds.getName()
                target_dataset = "Dataset:name:" + dataset_name
                # create new remote dataset
                uploaded_dataset_id = upload_dataset(cli, ds, remote_conn,
                                                     local_conn)

                remote_ds = remote_conn.getObject("Dataset", uploaded_dataset_id)
                images.extend(list(ds.listChildren()))
                if not images:
                    message += "No image found in dataset {}".format(
                        dataset_name)
                    return message

                print(("Processing {} images, in dataset {}".format(
                    len(images), dataset_name)))
                # TODO use remote_ds id, instead of target ds name
                uploaded_image_ids = upload_images(cli, images, managed_dir,
                                                   target_dataset, remote_conn, local_conn,
                                                   remote_ds, image_format)
        else:
            images = objects

            print(("Processing %s images" % len(images)))
            uploaded_image_ids = upload_images(cli, images, managed_dir,
                                               None, remote_conn, local_conn, None, image_format)
    finally:
        close_remote_connection(c, cli, remote_conn)
    # End of transferring images

    message += "uploaded image ids: " + str(tuple(uploaded_image_ids))
    return message


def connect_to_remote(password, username):
    c = omero.client(host=REMOTE_HOST, port=REMOTE_PORT,
                     args=["--Ice.Config=/dev/null", "--omero.debug=1"])
    c.createSession(username, password)
    remote_conn = BlitzGateway(client_obj=c)
    cli = omero.cli.CLI()
    cli.loadplugins()
    cli.set_client(c)
    del os.environ["ICE_CONFIG"]
    return c, cli, remote_conn


def close_remote_connection(c, cli, remote_conn):
    remote_conn.close()
    c.closeSession()
    cli.close()


def upload_dataset(cli, ds, remote_conn, local_conn):
    temp_file = NamedTemporaryFile().name
    # This temp_file is a work around to get hold of the id of uploaded
    # datasets from stdout.

    name_cmd = 'name=' + ds.getName()
    desc_cmd = "description="+ ds.getDescription()
    with open(temp_file, 'w+') as tf, stdout_redirected(tf):
            # bin/omero obj new Dataset name='new_dataset'
            cli.onecmd(["obj", "new", "Dataset", name_cmd, desc_cmd])

    with open(temp_file, 'r') as tf:
        txt = tf.readline()
        uploaded_dataset_id = re.findall(r'\d+', txt)[0]
    print("uploaded dataset ", uploaded_dataset_id)
    remote_ds = remote_conn.getObject("Dataset", uploaded_dataset_id)
    # TODO add description and tags for dataset
    add_attachments(ds, remote_ds, remote_conn, local_conn)
    return uploaded_dataset_id


def save_plane(image, format, c_name, z_range, project_z, t=0, channel=None,
               greyscale=False, zoom_percent=None, folder_name=None):
    """
    Renders and saves an image to disk.

    @param image:           The image to render
    @param format:          The format to save as
    @param c_name:          The name to use
    @param z_range:         Tuple of (zIndex,) OR (zStart, zStop) for
                            projection
    @param t:               T index
    @param channel:         Active channel index. If None, use current
                            rendering settings
    @param greyscale:       If true, all visible channels will be
                            greyscale
    @param zoom_percent:    Resize image by this percent if specified
    @param folder_name:     Indicate where to save the plane
    """

    original_name = image.getName()
    log("")
    log("save_plane..")
    log("channel: %s" % c_name)
    log("z: %s" % z_range)
    log("t: %s" % t)

    img_name = ''

    # if channel == None: use current rendering settings
    if channel is not None:
        image.setActiveChannels([channel+1])    # use 1-based Channel indices
        if greyscale:
            image.setGreyscaleRenderingModel()
        else:
            image.setColorRenderingModel()
    if project_z:
        # imageWrapper only supports projection of full Z range (can't
        # specify)
        image.setProjection('intmax')

    # All Z and T indices in this script are 1-based, but this method uses
    # 0-based.
    plane = image.renderImage(z_range[0]-1, t-1)
    if zoom_percent:
        w, h = plane.size
        fraction = (float(zoom_percent) / 100)
        plane = plane.resize((int(w * fraction), int(h * fraction)),
                             Image.ANTIALIAS)

    if format == "PNG":
        img_name = make_image_name(
            original_name, c_name, z_range, t, "png", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name, "PNG")
    elif format == 'TIFF':
        img_name = make_image_name(
            original_name, c_name, z_range, t, "tiff", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name, 'TIFF')
    else:
        img_name = make_image_name(
            original_name, c_name, z_range, t, "jpg", folder_name)
        log("Saving image: %s" % img_name)
        plane.save(img_name)

    return img_name


def make_image_name(original_name, c_name, z_range, t, extension, folder_name):
    """
    Produces the name for the saved image.
    E.g. imported/myImage.dv -> myImage_DAPI_z13_t01.png
    """
    name = os.path.basename(original_name)
    # name = name.rsplit(".",1)[0]  # remove extension
    if len(z_range) == 2:
        z = "%02d-%02d" % (z_range[0], z_range[1])
    else:
        z = "%02d" % z_range[0]
    img_name = "%s_%s_z%s_t%02d.%s" % (name, c_name, z, t, extension)
    if folder_name is not None:
        img_name = os.path.join(folder_name, img_name)
    # check we don't overwrite existing file
    i = 1
    name = img_name[:-(len(extension)+1)]
    while os.path.exists(img_name):
        img_name = "%s_(%d).%s" % (name, i, extension)
        i += 1
    return img_name


def save_as_ome_tiff(conn, image, folder_name=None):
    """
    Saves the image as an ome.tif in the specified folder
    """

    extension = "ome.tif"
    name = os.path.basename(image.getName())
    img_name = "%s.%s" % (name, extension)
    if folder_name is not None:
        img_name = os.path.join(folder_name, img_name)
    # check we don't overwrite existing file
    i = 1
    path_name = img_name[:-(len(extension)+1)]
    while os.path.exists(img_name):
        img_name = "%s_(%d).%s" % (path_name, i, extension)
        i += 1

    log("  Saving file as: %s" % img_name)
    file_size, block_gen = image.exportOmeTiff(bufsize=65536)
    with open(str(img_name), "wb") as f:
        for piece in block_gen:
            f.write(piece)


def save_planes_for_image(conn, image, size_c, split_cs, merged_cs,
                          channel_names=None, z_range=None, t_range=None,
                          greyscale=False, zoom_percent=None, project_z=False,
                          format="PNG", folder_name=None):
    """
    Saves all the required planes for a single image, either as individual
    planes or projection.

    @param renderingEngine:     Rendering Engine, NOT initialised.
    @param queryService:        OMERO query service
    @param imageId:             Image ID
    @param zRange:              Tuple: (zStart, zStop). If None, use default
                                Zindex
    @param tRange:              Tuple: (tStart, tStop). If None, use default
                                Tindex
    @param greyscale:           If true, all visible channels will be
                                greyscale
    @param zoomPercent:         Resize image by this percent if specified.
    @param projectZ:            If true, project over Z range.
    """

    channels = []
    if merged_cs:
        # render merged first with current rendering settings
        channels.append(None)
    if split_cs:
        for i in range(size_c):
            channels.append(i)

    # set up rendering engine with the pixels
    """
    renderingEngine.lookupPixels(pixelsId)
    if not renderingEngine.lookupRenderingDef(pixelsId):
        renderingEngine.resetDefaults()
    if not renderingEngine.lookupRenderingDef(pixelsId):
        raise "Failed to lookup Rendering Def"
    renderingEngine.load()
    """

    if t_range is None:
        # use 1-based indices throughout script
        t_indexes = [image.getDefaultT()+1]
    else:
        if len(t_range) > 1:
            t_indexes = list(range(t_range[0], t_range[1]))
        else:
            t_indexes = [t_range[0]]

    img_name = ''

    c_name = 'merged'
    for c in channels:
        if c is not None:
            g_scale = greyscale
            if c < len(channel_names):
                c_name = channel_names[c].replace(" ", "_")
            else:
                c_name = "c%02d" % c
        else:
            # if we're rendering 'merged' image - don't want grey!
            g_scale = False
        for t in t_indexes:
            if z_range is None:
                default_z = image.getDefaultZ()+1
                img_name = save_plane(image, format, c_name, (default_z,), project_z, t,
                           c, g_scale, zoom_percent, folder_name)
            elif project_z:
                img_name = save_plane(image, format, c_name, z_range, project_z, t, c,
                           g_scale, zoom_percent, folder_name)
            else:
                if len(z_range) > 1:
                    for z in range(z_range[0], z_range[1]):
                        img_name = save_plane(image, format, c_name, (z,), project_z, t,
                                   c, g_scale, zoom_percent, folder_name)
                else:
                    img_name = save_plane(image, format, c_name, z_range, project_z, t,
                               c, g_scale, zoom_percent, folder_name)

    return img_name


def handle_pixels(local_conn, remote_conn, images, img, folder_name, pixels,
                  remote_ds=None, img_format='Hypercube'):
    # max size (default 12kx12k)
    size = remote_conn.getDownloadAsMaxSizeSetting()
    size = int(size)

    # script parameters used in 'Batch_Image_Export', set to defaults
    project_z = None
    default_z_option = 'Default-Z (last-viewed)'
    zoom_percent = 100.00
    greyscale = False
    split_cs = True
    merged_cs = True
    channel_names = []

    # functions used below for each imaage.
    def get_z_range(size_z, script_params):
        z_range = None
        if "Choose_Z_Section" in script_params:
            z_choice = script_params["Choose_Z_Section"]
            # NB: all Z indices in this script are 1-based
            if z_choice == 'ALL Z planes':
                z_range = (1, size_z + 1)
            elif "OR_specify_Z_index" in script_params:
                z_index = script_params["OR_specify_Z_index"]
                z_index = min(z_index, size_z)
                z_range = (z_index,)
            elif "OR_specify_Z_start_AND..." in script_params and \
                    "...specify_Z_end" in script_params:
                start = script_params["OR_specify_Z_start_AND..."]
                start = min(start, size_z)
                end = script_params["...specify_Z_end"]
                end = min(end, size_z)
                # in case user got z_start and z_end mixed up
                z_start = min(start, end)
                z_end = max(start, end)
                if z_start == z_end:
                    z_range = (z_start,)
                else:
                    z_range = (z_start, z_end + 1)
        return z_range

    def get_t_range(size_t, script_params):
        t_range = None
        if "Choose_T_Section" in script_params:
            t_choice = script_params["Choose_T_Section"]
            # NB: all T indices in this script are 1-based
            if t_choice == 'ALL T planes':
                t_range = (1, size_t + 1)
            elif "OR_specify_T_index" in script_params:
                t_index = script_params["OR_specify_T_index"]
                t_index = min(t_index, size_t)
                t_range = (t_index,)
            elif "OR_specify_T_start_AND..." in script_params and \
                    "...specify_T_end" in script_params:
                start = script_params["OR_specify_T_start_AND..."]
                start = min(start, size_t)
                end = script_params["...specify_T_end"]
                end = min(end, size_t)
                # in case user got t_start and t_end mixed up
                t_start = min(start, end)
                t_end = max(start, end)
                if t_start == t_end:
                    t_range = (t_start,)
                else:
                    t_range = (t_start, t_end + 1)
        return t_range

    img_name = ''

    if img_format == 'Hypercube':
        try:
            original = local_conn.getObject("Image", img.id)
            desc = original.getDescription()
            name = original.getName()
            sizeZ = original.getSizeZ()
            sizeC = original.getSizeC()
            sizeT = original.getSizeT()
            zctList = []
            for z in range(sizeZ):
                for c in range(sizeC):
                    for t in range(sizeT):
                        zctList.append((z, c, t))

            def planeGen():
                planes = original.getPrimaryPixels().getPlanes(zctList)
                for p in planes:
                    # perform some manipulation on each plane
                    yield p

            remote_img = remote_conn.createImageFromNumpySeq(planeGen(),
                                                             name, sizeZ=sizeZ, sizeC=sizeC,
                                                             sizeT=sizeT, description=desc,
                                                             dataset=remote_ds)

            return remote_img.id
        except Exception as e:
            msg = "Cannot transfer raw pixel image: %s: %s" % (
                img.getName(), e)

    elif img_format == 'OME-TIFF':
        if img._prepareRE().requiresPixelsPyramid():
            log("  ** Can't export a 'Big' image to OME-TIFF. **")
            if len(images) == 1:
                return None, "Can't export a 'Big' image to %s." % format
            return
        else:
            save_as_ome_tiff(remote_conn, img, folder_name)
    else:
        size_x = pixels.getSizeX()
        size_y = pixels.getSizeY()

        if size_x * size_y > size:
            msg = "Can't export image over %s pixels. " \
                  "See 'omero.client.download_as.max_size'" % size
            log("  ** %s. **" % msg)
            if len(images) == 1:
                return None, msg
            return
        else:
            log("Exporting image as %s: %s" % (format, img.getName()))

        log("\n----------- Saving planes from image: '%s' ------------"
            % img.getName())
        size_c = img.getSizeC()
        size_z = img.getSizeZ()
        size_t = img.getSizeT()

        script_params = {'Choose_T_Section': 'ALL T planes', 'Choose_Z_Section':
                         'ALL Z planes'}
        z_range = get_z_range(size_z, script_params)
        t_range = get_t_range(size_t, script_params)

        log("Using:")
        if z_range is None:
            log("  Z-index: Last-viewed")
        elif len(z_range) == 1:
            log("  Z-index: %d" % z_range[0])
        else:
            log("  Z-range: %s-%s" % (z_range[0], z_range[1] - 1))
        if project_z:
            log("  Z-projection: ON")
        if t_range is None:
            log("  T-index: Last-viewed")
        elif len(t_range) == 1:
            log("  T-index: %d" % t_range[0])
        else:
            log("  T-range: %s-%s" % (t_range[0], t_range[1] - 1))
        log("  Format: %s" % format)
        if zoom_percent is None:
            log("  Image Zoom: 100%")
        else:
            log("  Image Zoom: %s" % zoom_percent)
        log("  Greyscale: %s" % greyscale)
        log("Channel Rendering Settings:")
        for ch in img.getChannels():
            log("  %s: %d-%d"
                % (ch.getLabel(), ch.getWindowStart(), ch.getWindowEnd()))

        try:
            img_name = save_planes_for_image(remote_conn, img, size_c, split_cs, merged_cs,
                                  channel_names, z_range, t_range,
                                  greyscale, zoom_percent,
                                  project_z=project_z, format=img_format,
                                  folder_name=folder_name)
        finally:
            # Make sure we close Rendering Engine
            img._re.close()

    return img_name


def upload_images(cli, images, managed_dir, target_dataset, remote_conn,
                  local_conn, remote_ds=None, image_format="Hypercube"):
    uploaded_image_ids = []
    uploaded_image_id = ''

    for image in images:
        print(("Processing image: ID %s: %s" % (image.id, image.getName())))
        desc = image.getDescription()
        print("Description: ", desc)
        temp_file = NamedTemporaryFile().name

        if len(list(image.getImportedImageFiles())) > 0:
            # TODO haven't tested an image with multiple files -see fileset.
            for f in image.getImportedImageFiles():
                file_loc = os.path.join(managed_dir, f.path, f.name)
                # This temp_file is a work around to get hold of the id of uploaded
                # images from stdout.
                with open(temp_file, 'w+') as tf, stdout_redirected(tf):
                    if target_dataset:
                        cli.onecmd(["import", file_loc, '-T', target_dataset,
                                    '--description', desc, '--no-upgrade-check'])
                    else:
                        cli.onecmd(["import", file_loc, '--description', desc,
                                    '--no-upgrade-check'])

                with open(temp_file, 'r') as tf:
                    txt = tf.readline()
                    # assert txt.startswith("Image:")
                    uploaded_image_id = re.findall(r'\d+', txt)[0]
        else:
            ids = []

            # no imported images, so must generate the planes/channels instead
            pixels = image.getPrimaryPixels()
            if (pixels.getId() in ids):
                continue
            ids.append(pixels.getId())

            if image_format == 'Hypercube':
                uploaded_image_id = handle_pixels(local_conn, remote_conn, images, image,
                                         managed_dir, pixels, remote_ds, image_format)
            else:
                file_loc = handle_pixels(local_conn, remote_conn, images, image,
                                         managed_dir, pixels, remote_ds, image_format)

                # write log for exported images (not needed for ome-tiff)
                name = 'Batch_Image_Export.txt'
                with open(os.path.join(managed_dir, name), 'w') as log_file:
                    for s in log_strings:
                        log_file.write(s)
                        log_file.write("\n")

                # This temp_file is a work around to get hold of the id of uploaded
                # images from stdout.
                with open(temp_file, 'w+') as tf, stdout_redirected(tf):
                    if target_dataset:
                        cli.onecmd(["import", file_loc, '-T', target_dataset,
                                    '--description', desc, '--no-upgrade-check'])
                    else:
                        cli.onecmd(["import", file_loc, '--description', desc,
                                    '--no-upgrade-check'])

                with open(temp_file, 'r') as tf:
                    txt = tf.readline()
                    # assert txt.startswith("Image:")
                    uploaded_image_id = re.findall(r'\d+', txt)[0]

        uploaded_image_ids.append(uploaded_image_id)

        # TODO check what happens when an image has multiple files.
        remote_image = remote_conn.getObject("Image", uploaded_image_id)
        add_channel_labels(image, remote_image, remote_conn)
        add_attachments(image, remote_image, remote_conn, local_conn, remote_ds)

    print("ids are: ", uploaded_image_ids)
    return uploaded_image_ids


# https://github.com/openmicroscopy/omero-example-scripts/blob/master/processing_scripts/Transform_Image.py
def add_channel_labels(local_image, remote_image, remote_conn):
    # Get channel Colors and Names to apply to the new Image
    cNames = [c.getLabel() for c in local_image.getChannels()]

    update_service = remote_conn.c.sf.getUpdateService()
    query_service = remote_conn.c.sf.getQueryService()

    # Apply colors from the original image to the new one
    for i, c in enumerate(remote_image.getChannels()):
        lc = c.getLogicalChannel()
        lc.setName(cNames[i])
        lc.save()

        # need to reload channels to avoid optimistic lock on update
        cObj = query_service.get("Channel", c.id)
        update_service.saveObject(cObj)


def add_attachments(local_item, remote_item, remote_conn, local_conn,
                    remote_ds=None):
    # Sometimes the image already has the tags uploaded.
    global pre_loaded_tags

    # Finding these so we can check for duplications.
    for existing_ann in remote_item.listAnnotations():
        if existing_ann.OMERO_TYPE == omero.model.TagAnnotationI:
            print("Tag is already there: ", existing_ann.getTextValue())

            if not existing_ann.getTextValue() in pre_loaded_tags:
                pre_loaded_tags.append(existing_ann.getTextValue())

    if remote_ds != None:
        for existing_ann in remote_ds.listAnnotations():
            if existing_ann.OMERO_TYPE == omero.model.TagAnnotationI:
                print("Tag is already there: ", existing_ann.getTextValue())

                if not existing_ann.getTextValue() in pre_loaded_tags:
                    pre_loaded_tags.append(existing_ann.getTextValue())

    for ann in local_item.listAnnotations():
        remote_ann = None

        if ann.OMERO_TYPE == omero.model.TagAnnotationI:
            tags = remote_conn.getObjects("TagAnnotation",
                                          attributes={'textValue': ann.getTextValue()})

            for tag in tags:
                remote_ann = tag

                if not ann.getTextValue() in pre_loaded_tags:
                    pre_loaded_tags.append(ann.getTextValue())

                break
            else:
                # There's an example with data from a batch export.
                # It uploads the tags with the image upload, and
                # then this adds duplicate ones.
                if not ann.getTextValue() in pre_loaded_tags:
                    remote_ann = omero.gateway.TagAnnotationWrapper(remote_conn)
                    remote_ann.setValue(ann.getTextValue())
                    pre_loaded_tags.append(ann.getTextValue())
        elif ann.OMERO_TYPE == omero.model.CommentAnnotationI:
            remote_ann = omero.gateway.CommentAnnotationWrapper(remote_conn)
            remote_ann.setValue(ann.getTextValue())
        elif ann.OMERO_TYPE == omero.model.LongAnnotationI:  # rating
            remote_ann = omero.gateway.LongAnnotationWrapper(remote_conn)
            remote_ann.setNs(ann.getNs())
            remote_ann.setValue(ann.getValue())
        elif ann.OMERO_TYPE == omero.model.MapAnnotationI:
            remote_ann = omero.gateway.MapAnnotationWrapper(remote_conn)
            remote_ann.setNs(ann.getNs())
            remote_ann.setValue(ann.getValue())
        elif ann.OMERO_TYPE == omero.model.FileAnnotationI and include_annos:
            file_to_upload = ann.getFile()
            anno_file_path = file_to_upload.getPath()

            # if file_path is a real path, it's a real file, otherwise it's a RawFileStore
            # this never seems to work so it's disabled in favour of RawFileStore
            if len(anno_file_path.strip()) < -1:
                file_path = os.path.join(anno_file_path,
                                         file_to_upload.getName())

                mime = file_to_upload.getMimetype()
                namespace = ann.getNs()
                description = ann.getDescription()
                remote_ann = remote_conn.createFileAnnfromLocalFile(
                    file_path, mimetype=mime, ns=namespace, desc=description)
                # TODO this message would be better if it said if adding to image or dataset
                print("Attaching FileAnnotation to Item: ", "File ID:",\
                    remote_ann.getId(),  ",", remote_ann.getFile().getName(), \
                    "Size:", remote_ann.getFile().getSize())
            else:
                local_raw_file_store = local_conn.c.sf.createRawFileStore()
                local_raw_file_store.setFileId(file_to_upload.getId())

                remote_raw_file_store = remote_conn.c.sf.createRawFileStore()

                update_service = remote_conn.c.sf.getUpdateService()
                file_path = os.path.join(file_to_upload.getPath(),
                                         file_to_upload.getName())
                mime = file_to_upload.getMimetype()
                namespace = ann.getNs()
                description = ann.getDescription()
                file_size = local_raw_file_store.size()

                new_file_anno = omero.model.OriginalFileI()
                new_file_anno.setName(rstring(file_to_upload.getName()))
                new_file_anno.setPath(rstring(file_path))
                if mime:
                    new_file_anno.mimetype = rstring(mime)
                new_file_anno.setSize(rlong(file_size))
                new_file_anno = update_service.saveAndReturnObject(new_file_anno)

                remote_raw_file_store.setFileId(new_file_anno.getId().getValue())

                try:
                    pos = 0
                    chunk_size = 1024 * 1024 * 10  # 10 MB

                    while True:
                        if chunk_size > file_size - pos:
                            chunk_size = file_size - pos

                        buf = local_raw_file_store.read(pos, chunk_size)
                        if buf:
                            remote_raw_file_store.write(buf, pos, chunk_size)
                            pos = pos + chunk_size
                        else:
                            break

                    uploaded_file = remote_raw_file_store.save()

                    fa = omero.model.FileAnnotationI()
                    fa.setDescription(omero.rtypes.rstring(description))
                    fa.setNs(omero.rtypes.rstring(namespace))
                    fa.setFile(uploaded_file)
                    fa = update_service.saveAndReturnObject(fa)

                    link = None
                    # put the image in dataset, if specified.
                    if isinstance(remote_item, omero.gateway.ImageWrapper): # need to check if it's image or dataset?
                        link = omero.model.ImageAnnotationLinkI()
                        link.parent = omero.model.ImageI(remote_item.id, False)
                    elif isinstance(remote_item, omero.gateway.DatasetWrapper):
                        link = omero.model.DatasetAnnotationLinkI()
                        link.parent = omero.model.DatasetI(remote_item.id, False)

                    link.child = omero.model.FileAnnotationI(fa.id, False)
                    update_service.saveAndReturnObject(link)
                except Exception as e:
                    msg = "Cannot save the file annotation: %s: %s" % (
                        file_to_upload.getName(), e)
                finally:
                    local_raw_file_store.close()
                    remote_raw_file_store.close()
        else:
            remote_ann = omero.gateway.CommentAnnotationWrapper(remote_conn)
            comment = 'Annotation of type: {} could not be uploaded.'.\
                format(ann.OMERO_TYPE)
            remote_ann.setValue(comment)
        if remote_ann:
            remote_ann.save()
            remote_item.linkAnnotation(remote_ann)


if __name__ == "__main__":
    run_script()