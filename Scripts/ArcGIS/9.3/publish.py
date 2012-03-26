# ---------------------------------------------------------------------------
# publish.py
# Publish a feature class from ArcSDE to the OpenColorado Data Catalog.
#
# This script completes the following:
#  1) Exports the ArcSDE feature class to the download folder
#     in the following formats:
#    a. Shapefile (zipped)
#    b. CAD (dwg file)
#    c. KML (zipped KMZ)
#  2) Updates the timestamp of the dataset on the CKAN repository
#     catalog (if the dataset is present)
#  3) The script automatically manages the creation of output folders if they
#     do not already exist.  Also creates temp folders for processing as
#     needed.
#  4) The output folder has the following structure.  You can start with an 
#     empty folder and the script will create the necessary directories.
#        <catalog_publish_folder>
#           |- <dataset_name> (catalog dataset name with prefix removed, dashes 
#                              replaced with underscores)
#               |- shape
#                   |- <dataset_name>.shp
#               |- cad
#                   |- <dataset_name>.dwg
#               |- kml 
#                   |- <dataset_name>.kmz
# ---------------------------------------------------------------------------

# Import system modules
import sys, string, os, arcgisscripting, shutil, zipfile, glob, ckanclient, datetime, argparse

# ******************************
# User-configurable settings

# ----------------
# Catalog settings
# ----------------

# Base URL from the catalog API
ckan_api = 'http://colorado.ckan.net/api/2/'

# The API key (find by visiting http://colorado.ckan.net/user/me when logged in to colorado.ckan.net)
ckan_api_key = ''

# The API prefix used for all datasets on the catalog
ckan_dataset_prefix = ""

# ----------------
# Data settings
# ----------------

# Database connection to the location of the source data
database_connection = "Database Connections\\SDE Connection.sde"

# The folder that exported files will be published to
catalog_publish_folder = "C:\\DataCatalog\\Download\\"

# The name of the temporary folder for creating export files before publishing
temp_folder = "temp\\"

# ******************************

# Other global variables
args = None
gp = None

def main():
    """Main function
    
    Returns:
      None
    """
    global args
    
    parser = argparse.ArgumentParser(description='Publish a feature class from ArcSDE to OpenColorado.')
    parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
    
    # Optional arguments
    parser.add_argument('-v',
        action='store_true', 
        dest='verbose', 
        help='Verbose output messages')
    
    parser.add_argument('-f',
        action='append',
        dest='formats', 
        choices=['shp','dwg','kml'],
        help='Specific formats to publish (shp=Shapefile, dwg=CAD drawing file, kml=Keyhole Markup Language)')
    
    parser.add_argument('-s',
        action='store_true', 
        dest='suppress_update_version',
        default=False,
        help='Do not update the dataset version on OpenColorado')
        
    # Positional arguments
    parser.add_argument('feature_class',
        action='store',
        help='The fully qualified name of the feature class in ArcSDE')
        
    parser.add_argument('catalog_dataset',
        action='store',
        help='The name of the dataset on OpenColorado (without the "' + ckan_dataset_prefix + '" prefix)')

    args = parser.parse_args()
    
    # If no formats are specified then enable all formats
    if args.formats == None:
        args.formats = 'shp','dwg','kml'
        
    info('Starting ' + sys.argv[0])
    debug(' Feature class: ' + args.feature_class)
    debug(' Catalog dataset: ' + args.catalog_dataset)
    debug(' ArcSDE Connection: ' + database_connection)
    debug(' Publish folder: ' + catalog_publish_folder)
                    
    # Initialize geoprocessing tools
    debug('Initializing geoprocessing tools')
    gp = initialize_geoprocessing()
    
    # Create the dataset folder
    create_dataset_folder()
    
    # Create temporary folder for processing
    create_dataset_temp_folder()

    # Export to the various file formats
    if 'shp' in args.formats:
        info('Exporting to shapefile')
        export_shapefile()
    
    if 'dwg' in args.formats:
        info('Exporting to CAD drawing file')
        export_cad()

    if 'kml' in args.formats:
        info('Exporting to kml file')
        export_kml()
    
    # Update the dataset version on the ckan repository (causes the last modified date to be updated)
    if args.suppress_update_version == False:
        info('Updating dataset version')
        update_dataset_version()
    
    # Delete the dataset temp folder
    delete_dataset_temp_folder()
    
    info('Completed ' + sys.argv[0])
    
def create_folder(directory, delete=False):
    """Creates a folder if it does not exist

    Returns:
      None
    """
    
    if os.path.exists(directory) and delete:
        debug('Deleting directory "' + directory)
        shutil.rmtree(directory)
        
    if not os.path.exists(directory):
        debug('Directory "' + directory + '" does not exist.  Creating..')
        os.makedirs(directory)
        
    return directory
        
def create_dataset_folder():
    """Creates the output folder for exported files.
    
    Creates the output folder if it does not exist.

    Returns:
      None
    """
    directory = catalog_publish_folder + get_dataset_filename()
    create_folder(directory)

def create_dataset_temp_folder():
    """Creates a temporary folder for processing data
    
    Creates the temporary folder if it does not exist.

    Returns:
      None
    """
    directory = catalog_publish_folder + get_dataset_filename() + "//" + temp_folder
    create_folder(directory)

def delete_dataset_temp_folder():
    """Deletes the temporary folder for processing data

    Returns:
      None
    """
    directory = catalog_publish_folder + get_dataset_filename() + "//" + temp_folder
    if os.path.exists(directory):
        debug('Deleting directory "' + directory)
        shutil.rmtree(directory)
    
def initialize_geoprocessing():
    """Initializes geoprocessing tools. 

    Returns:
      None
    """
    global gp
    
    # Create the Geoprocessor object
    gp = arcgisscripting.create()
    
    # Check out any necessary licenses
    gp.CheckOutExtension("3D")
    
    # Load required toolboxes...
    gp.AddToolbox("C:/Program Files (x86)/ArcGIS/ArcToolbox/Toolboxes/Conversion Tools.tbx")
    gp.AddToolbox("C:/Program Files (x86)/ArcGIS/ArcToolbox/Toolboxes/Data Management Tools.tbx")
   
    return gp

def publish_file(directory, file, type):
    """Publishes a file to the catalog download folder
    
    Returns:
      None
    """
    
    folder = create_folder(catalog_publish_folder + get_dataset_filename())
    
    folder = create_folder(folder + "\\" + type)
    
    info(' Copying ' + file + ' to ' + folder)
    shutil.copyfile(directory + "/" + file, folder + "/" + file)

def get_dataset_filename():
    """Gets a file system friendly name from the catalog dataset name
    
    Returns:
      None
    """
    global args
    return args.catalog_dataset.replace("-","_")
    
def export_shapefile():
    """Exports the feature class as a zipped shapefile
    
    Returns:
      None
    """
    folder = 'shape'
    name = get_dataset_filename()
    
    # Create a shape folder in the temp directory if it does not exist
    working_folder = catalog_publish_folder + name + "//" + temp_folder + "//" + folder
    create_folder(working_folder, True)

    # Create a folder for the shapefile (since it is a folder)
    create_folder(working_folder + "\\" + name)
    
    # Export the shapefile to the folder
    source = database_connection + "\\" + args.feature_class
    destination = working_folder + "\\" + name + "\\" + name + ".shp"
    
    # Export the shapefile
    debug(' - Exporting to shapefile from "' + source + '" to "' + destination + '"')
    gp.CopyFeatures_management(source, destination, "", "0", "0", "0")
    
    # Zip up the files
    debug(' - Zipping the shapefile')
    zip = zipfile.ZipFile(working_folder + "\\" + name + ".zip", "w")
    
    for filename in glob.glob(working_folder + "/" + name + "/*"):
        zip.write(filename, os.path.basename(filename), zipfile.ZIP_DEFLATED)
        
    zip.close()
    
    # Publish the zipfile to the download folder
    publish_file(working_folder, name + ".zip","shape")
    
def export_cad():
    """Exports the feature class as a CAD drawing file
    
    Returns:
      None
    """
    folder = 'cad'
    name = get_dataset_filename()
    
    # Create a cad folder in the temp directory if it does not exist
    working_folder = catalog_publish_folder + name + "//" + temp_folder + "//" + folder
    create_folder(working_folder, True)
    
    # Export the shapefile to the folder
    source = database_connection + "\\" + args.feature_class
    destination = working_folder + "\\" + name + ".dwg"
    
    # Export the drawing file
    debug(' - Exporting to DWG file from "' + source + '" to "' + destination + '"')
    gp.ExportCAD_conversion(source, "DWG_R2000", destination, "Ignore_Filenames_in_Tables", "Overwrite_Existing_Files", "")
    
    # Publish the zipfile to the download folder
    publish_file(working_folder, name + ".dwg","cad")
    
def export_kml():
    """Exports the feature class to a kml file
    
    Returns:
      None
    """
    folder = 'cad'
    name = get_dataset_filename()
    
    # Create a cad folder in the temp directory if it does not exist
    working_folder = catalog_publish_folder + name + "//" + temp_folder + "//" + folder
    create_folder(working_folder, True)
    
    # Export the shapefile to the folder
    source = database_connection + "\\" + args.feature_class
    destination = working_folder + "\\" + name + ".kmz"
    
    # Make a feature layer (in memory)
    debug(' - Generating KML file in memory from  "' + source + '"')
    gp.MakeFeatureLayer_management(source, name, "", "")
    
    # Convert the layer to KML
    debug(' - Exporting KML file (KMZ) to "' + destination + '"')
    gp.LayerToKML_conversion(name, destination, "20000", "false", "DEFAULT", "1024", "96")

    # Publish the zipfile to the download folder
    publish_file(working_folder, name + ".kmz","kml")

def update_dataset_version():
    global args
    
    # Initialize ckan client
    ckan = ckanclient.CkanClient(base_location=ckan_api,api_key=ckan_api_key)
    
    # Create the name of the dataset on the CKAN instance
    dataset_id = ckan_dataset_prefix + args.catalog_dataset
    
    try:
        # Get the dataset
        dataset_entity = ckan.package_entity_get(dataset_id)
        
        # Increment the version number
        version = dataset_entity['version']
        version = increment_minor_version(version)
        dataset_entity['version'] = version
        
        # Update the dataset
        ckan.package_entity_put(dataset_entity)
        
    except ckanclient.CkanApiNotFoundError:
        info(" Dataset " + dataset_id + " not found on OpenColorado")
        
def increment_minor_version(version):
    incremented_version = version
    
    if version == None:
        incremented_version = "1.0.0"
        info ('No version number found.  Setting version to ' + incremented_version);
    else:
        version_parts = version.split(".")
        if len(version_parts) == 3:
            major = int(version_parts[0])
            minor = int(version_parts[1])
            revision = int(version_parts[2])
            incremented_version = str(major) + "." + str(minor + 1) + "." + str(revision)
            info ('Incrementing CKAN dataset version from ' + version + ' to ' + incremented_version);
    return incremented_version
            
def debug(message) :
    global args
    if args.verbose:
        log(message)

def info(message) :
    log(message)
    
def log(message):
    now = datetime.datetime.now()
    print now.strftime("%Y-%m-%d %H:%M") + ": " + message
    
    
#Execute main function    
if __name__ == '__main__':
    main()
    
