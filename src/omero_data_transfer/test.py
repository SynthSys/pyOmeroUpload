import random
from omero import client as om_client

client = om_client("demo.openmicroscopy.org", 4064)
conn = client.createSession('jhay', 'cooCheegh4qu')

cs = conn.getContainerService()
projects = cs.loadContainerHierarchy("Project", None, None)
for p in projects:                # omero.model.ProjectI
    print p.getName().getValue()     # need to 'unwrap' rstring
    for d in p.linkedDatasetList():
        print d.getName().getValue()

        # Retrieve all the images in the datasets as a Java List (index will start at 0)
        imageList = cs.getImages('Dataset', [d.getId().getValue()], None)
        for img in imageList:
            print img.getName().getValue()
            image = img

            z = image.sizeZ() / 2
            t = 0
            rendered_image = image.renderImage(z, t)
            # image = conn.getObject("Image", img.getId().getValue())
            # Initializes the Rendering engine and sets rendering settings
            #image.setActiveChannels([1, 2], [[20, 300], [50, 500]], ['00FF00', 'FF0000'])
            #pil_image = image.renderImage(0, 0)
            # Now we close the rendering engine
            #image._re.close

client.closeSession()