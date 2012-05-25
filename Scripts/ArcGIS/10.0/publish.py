# ---------------------------------------------------------------------------
# publish.py
# Publish a feature class from ArcSDE to the OpenColorado Data Catalog.  
# Publish in this case only means creating the output files that can be shared
# via a web server.  The script does *not* create the dataset on OpenColorado.
#
# If the dataset exists on OpenColorado its minor version number will be
# incremented. 
#
# This script completes the following:
# 1) Exports the ArcSDE feature class to the download folder
#    in the following formats:
#    a. Shapefile (zipped)
#    b. CAD (dwg file)
#    c. KML (zipped KMZ)
# 2) Updates the version number of the dataset on the CKAN repository
#    catalog (if the dataset is present)
# 3) The script automatically manages the creation of output folders if they
#    do not already exist.  Also creates temp folders for processing as
#    needed.
# 4) The output folder has the following structure.  You can start with an 
#    empty folder and the script will create the necessary directories.
#		<output_folder>
#		    |- <dataset_name> (catalog dataset name with prefix removed, dashes 
#							  replaced with underscores)
#			    |- shape
#				    |- <dataset_name>.shp
#		        |- cad
#				    |- <dataset_name>.dwg
#		        |- kml 
#				    |- <dataset_name>.kmz
# ---------------------------------------------------------------------------

# Import system modules
import sys, string, os, arcpy, shutil, zipfile, glob, ckanclient, datetime, argparse
from arcpy import env
import xml.etree.ElementTree as et

# Global variables
args = None
output_folder = None
source_feature_class = None

temp_folder = "temp"

def main():
	"""Main function
	
	Returns:
        None
	"""
	global args, output_folder, source_feature_class
	
	parser = argparse.ArgumentParser(description='Publish a feature class from ArcSDE to OpenColorado.')
	parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
			
	# Optional arguments	   
	parser.add_argument('-o', '--output-folder',
		action='store', 
		dest='output_folder',
		help='The root output folder in which to create the published files.  Sub-folders will automatically be created for each dataset (ex. C:\\\\temp).')
	
	parser.add_argument('-s', '--source-workspace',
		action='store', 
		dest='source_workspace', 
		required=True,
		help='The source workspace to publish the feature class from (ex. Database Connections\\\\SDE Connection.sde).  Backslashes must be escaped as in the example.')	
	
	parser.add_argument('-f',
		action='append',
		dest='formats', 
		choices=['shp','dwg','kml'],
		help='Specific formats to publish (shp=Shapefile, dwg=CAD drawing file, kml=Keyhole Markup Language).  If not specified all formats will be published.')
		
	parser.add_argument('-a', '--ckan-api',
		action='store', 
		dest='ckan_api', 
		default='http://colorado.ckan.net/api/2/',
		help='The root path to the CKAN repository (ex. http://colorado.ckan.net/api/2/)')
		
	parser.add_argument('-k', '--ckan-api-key',
		action='store', 
		dest='ckan_api_key',
		required=True,
		help='The CKAN API key (get from http://colorado.ckan.net/user/me when logged in)')
	
	parser.add_argument('-p', '--ckan-dataset-prefix',
		action='store', 
		dest='ckan_dataset_prefix',
		default='',
		help='A prefix used in conjunction with the catalog_dataset argument to create the complete dataset name on OpenColorado.')
	
	parser.add_argument('-i',
		action='store', 
		dest='increment',
		choices=['major','minor','revision','none'],
		default='revision',
		help='Update the version number on OpenColorado \n(default: %(default)s)')
	
	parser.add_argument('-m', '--update-from-metadata',
		action='store', 
		dest='update_from_metadata',
		choices=['description','tags','all'],
		help='Update dataset information using the source metadata')
		
	parser.add_argument('-v', '--verbose',
		action='store_true', 
		dest='verbose', 
		help='Verbose output messages')
		
	# Positional arguments
	parser.add_argument('feature_class',
		action='store',
		help='The fully qualified path to the feature class (ex. Database Connections\\\\SDE Connection.sde\\\\schema.parcels).  If a source workspace is specified (ex. -s Database Connections\\\\SDE Connection.sde) just the feature class name needs to be provided here (ex. schema.parcels)')
		
	parser.add_argument('catalog_dataset',
		action='store',
		help='The name of the dataset on OpenColorado.  If a prefix is provided (-p) don''t include it here.')
			
	args = parser.parse_args()
	
	# Set the global output folder (trim and append a slash to make sure the files get created inside the directory)
	if args.output_folder != None:
		output_folder = args.output_folder.strip()
	
	# Set the source feature class
	if args.source_workspace == None:
		source_feature_class = args.feature_class
	else:
		source_feature_class = os.path.join(args.source_workspace,args.feature_class)
			
	# If no formats are specified then enable all formats
	if args.formats == None:
		args.formats = 'shp','dwg','kml'
				
	info('Starting ' + sys.argv[0])
	debug(' Feature class: ' + source_feature_class)
	debug(' Catalog dataset: ' + args.catalog_dataset)
	debug(' Publish folder: ' + output_folder)
						
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
	
	# Update the dataset information on the ckan repository from the metadata
	if args.update_from_metadata != None:
		info('Updating dataset information from metadata')
		update_from_metadata()
	
	# Update the dataset version on the ckan repository (causes the last modified date to be updated)
	if args.increment != "none":
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
	directory = os.path.join(output_folder,get_dataset_filename())
	create_folder(directory)

def create_dataset_temp_folder():
	"""Creates a temporary folder for processing data
	
	Creates the temporary folder if it does not exist.

	Returns:
        None
	"""
	directory = os.path.join(output_folder,get_dataset_filename(),temp_folder)
	create_folder(directory)

def delete_dataset_temp_folder():
	"""Deletes the temporary folder for processing data

	Returns:
        None
	"""
	directory = os.path.join(output_folder,get_dataset_filename(),temp_folder)
	if os.path.exists(directory):
		debug('Deleting directory "' + directory)
		shutil.rmtree(directory)

def publish_file(directory, file, type):
	"""Publishes a file to the catalog download folder
	
	Returns:
        None
	"""
	
	folder = create_folder(os.path.join(output_folder,get_dataset_filename()))
	
	folder = create_folder(os.path.join(folder,type))
	
	info(' Copying ' + file + ' to ' + folder)
	shutil.copyfile(os.path.join(directory,file), os.path.join(folder,file))

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
	working_folder = os.path.join(output_folder,name,temp_folder,folder)
	create_folder(working_folder, True)

	# Create a folder for the shapefile (since it is a folder)
	create_folder(os.path.join(working_folder,name))
	
	# Export the shapefile to the folder
	source = source_feature_class
	destination = os.path.join(working_folder,name,name + ".shp")
	
	# Export the shapefile
	debug(' - Exporting to shapefile from "' + source + '" to "' + destination + '"')
	arcpy.CopyFeatures_management(source, destination, "", "0", "0", "0")
	
	# Zip up the files
	debug(' - Zipping the shapefile')
	zip = zipfile.ZipFile(os.path.join(working_folder,name + ".zip"), "w")
	
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
	working_folder = os.path.join(output_folder,name,temp_folder,folder)
	create_folder(working_folder, True)
	
	# Export the shapefile to the folder
	source = source_feature_class
	destination = os.path.join(working_folder,name + ".dwg")
	
	# Export the drawing file
	debug(' - Exporting to DWG file from "' + source + '" to "' + destination + '"')
	arcpy.ExportCAD_conversion(source, "DWG_R2000", destination, "Ignore_Filenames_in_Tables", "Overwrite_Existing_Files", "")
	
	# Publish the zipfile to the download folder
	publish_file(working_folder, name + ".dwg","cad")
	
def export_kml():
	"""Exports the feature class to a kml file
	
	Returns:
        None
	"""
	arcpy.CheckOutExtension("3D")
	
	folder = 'kml'
	name = get_dataset_filename()
	
	# Create a kml folder in the temp directory if it does not exist
	working_folder = os.path.join(output_folder,name,temp_folder,folder)
	create_folder(working_folder, True)
	
	# Export the feature class to a temporary file gdb
	source = source_feature_class
	destination = os.path.join(working_folder,name + ".kmz")
	
	gdb_temp = os.path.join(working_folder,name + ".gdb")
	gdb_feature_class = os.path.join(gdb_temp,name)
	
	debug(' - Creating temporary file geodatabase for processing:' + gdb_temp)
	if not arcpy.Exists(gdb_temp):
		arcpy.CreateFileGDB_management(os.path.dirname(gdb_temp),os.path.basename(gdb_temp)) 
		
	debug(' - Copying feature class to:' + gdb_feature_class)
	arcpy.CopyFeatures_management(source, gdb_feature_class)

	# Make a feature layer (in memory)
	debug(' - Generating KML file in memory from  "' + gdb_feature_class + '"')
	arcpy.MakeFeatureLayer_management(gdb_feature_class, name, "", "")
	
	# Replace any literal nulls <Null> with empty as these don't convert to KML correctly
	replace_literal_nulls(name)
   
	# Convert the layer to KML
	debug(' - Exporting KML file (KMZ) to "' + destination + '"')
	arcpy.LayerToKML_conversion(name, destination, "20000", "false", "DEFAULT", "1024", "96")
		
	# Delete the in-memory feature layer and the file geodatabase
	debug(' - Deleting in-memory feature layer:' + name)
	arcpy.Delete_management(name)
	
	debug(' - Deleting temporary file geodatabase:' + gdb_temp)
	arcpy.Delete_management(gdb_temp)
	
	# Publish the zipfile to the download folder
	publish_file(working_folder, name + ".kmz","kml")

def update_dataset_version():
	global args
	
	# Initialize ckan client
	ckan = ckanclient.CkanClient(base_location=args.ckan_api,api_key=args.ckan_api_key)
	
	# Create the name of the dataset on the CKAN instance
	dataset_id = args.ckan_dataset_prefix + args.catalog_dataset
	
	try:
		# Get the dataset
		dataset_entity = ckan.package_entity_get(dataset_id)
		
		# Increment the version number
		version = dataset_entity['version']
		version = increment_version(version, args.increment)
		dataset_entity['version'] = version
		
		# Update the dataset
		ckan.package_entity_put(dataset_entity)
		
	except ckanclient.CkanApiNotFoundError:
		info(" Dataset " + dataset_id + " not found on OpenColorado")
		
def update_from_metadata():
	global args, env
	
	folder = 'metadata'
	name = get_dataset_filename()
	
	# Initialize ckan client
	ckan = ckanclient.CkanClient(base_location=args.ckan_api,api_key=args.ckan_api_key)
	
	# Create the name of the dataset on the CKAN instance
	dataset_id = args.ckan_dataset_prefix + args.catalog_dataset
	
	#try:
	#	# Get the dataset
	#	dataset_entity = ckan.package_entity_get(dataset_id)
					  
	#except ckanclient.CkanApiNotFoundError:
	#	info(" Dataset " + dataset_id + " not found on OpenColorado")
	
	#if dataset_entity != None:
	
	# Create a kml folder in the temp directory if it does not exist
	working_folder = os.path.join(output_folder,name,temp_folder,folder)
	create_folder(working_folder, True)
	
	# Export the feature class to a temporary file gdb
	source = source_feature_class
	destination = os.path.join(working_folder,name + ".xml")
	
	# Export the metadata
	env.workspace = "C:/temp"
	dir = arcpy.GetInstallInfo("desktop")["InstallDir"]
	translator = dir + "Metadata/Translator/ESRI_ISO2ISO19139.xml"
	
	arcpy.ExportMetadata_conversion(source, translator, destination)
	
	# Publish the metadata to the download folder
	publish_file(working_folder, name + ".xml","metadata")
	
	metadata_file = open(destination,"r")
	metadata_xml = et.parse(metadata_file)
 
	# Specify the namespaces
	ns_gmd = "{http://www.isotc211.org/2005/gmd}"
	ns_gco = "{http://www.isotc211.org/2005/gco}"
	
	# Get the abstract
	xpath_abstract = '{0}identificationInfo/{0}MD_DataIdentification/{0}abstract/{1}CharacterString'.format(ns_gmd,ns_gco);
	abstract = metadata_xml.find(xpath_abstract).text
	#print abstract
	
	# Get the keywords
	xpath_keywords = '//{0}MD_Keywords/{0}keyword/{1}CharacterString'.format(ns_gmd,ns_gco);
	keyword_elements = metadata_xml.findall(xpath_keywords)
	for keyword_element in keyword_elements:
		keyword = keyword_element.text
		keyword = keyword.lower().replace(' ','-')
	#	print keyword
	 
	# TODO: Update the dataset and push the info to the catalog
	
	# Update the dataset
	#ckan.package_entity_put(dataset_entity)

def increment_version(version, increment_type):
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
			
			if increment_type == 'major':
				major = major + 1
			elif increment_type == 'minor':
				minor = minor + 1
			elif increment_type == 'revision':	  
				revision = revision + 1
				
			incremented_version = str(major) + "." + str(minor) + "." + str(revision)
			info ('Incrementing CKAN dataset version from ' + version + ' to ' + incremented_version);
	return incremented_version

def replace_literal_nulls(feature_layer):
	
	debug(' - Replacing <Null> with empty string in all string fields')
	fieldList = arcpy.ListFields(feature_layer)
	
	
	for field in fieldList:
		if field.type == 'String':
			debug(' - Replacing <Null> with empty string in field: ' + field.name)
			expression = '!' + field.name + '!.replace("<Null>","")'
			arcpy.CalculateField_management(feature_layer, field.name, expression,"PYTHON")
	
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
	
