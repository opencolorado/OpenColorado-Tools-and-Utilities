# ---------------------------------------------------------------------------
# publish.py
# ---------------------------------------------------------------------------
# Publish a feature class from ArcSDE to the OpenColorado Data Catalog.
# The publishing process creats output files in a variety of formats that 
# can be shared via a web server. The script uses the CKAN client API to
# create the dataset on OpenColorado if it does not already exist. 
# If the dataset exists, its revision number will be incremented.
#----------------------------------------------------------------------------
# This script completes the following:
# 1) Exports the ArcSDE feature class to the download folder
#    in the following formats:
#
#    a. Shapefile (zipped)
#    b. CAD (dwg file)
#    c. KML (zipped KMZ)
#	 d. ArcGIS Metadata (xml)
#
# 	 The script automatically manages the creation of output folders if they
#    do not already exist.  Also creates temp folders for processing as
#    needed. The output folder has the following structure. You can start
#    with an empty folder and the script will create the necessary 
#	 directories.
#
#		<output_folder>
#		    |- <dataset_name> (catalog dataset name with prefix removed, 
#							   dashes replaced with underscores)
#			    |- shape
#				    |- <dataset_name>.shp
#		        |- cad
#				    |- <dataset_name>.dwg
#		        |- kml 
#				    |- <dataset_name>.kmz
#		        |- metadata 
#				    |- <dataset_name>.xml
#
# 2) Reads the exported ArcGIS Metadata xml file and parses the relevant
#	 metadata fields to be published to the OpenColorado Data Repository.
#
# 3) Uses the CKAN client API to create a new dataset on the OpenColorado
#	 Data Repository if the dataset does not already exist. If the dataset
#	 already exists, it is updated. 
#
# 4) Updates the version (revision) number of the dataset on the OpenColorado
#     Data Catalog (if it already exists)
# ---------------------------------------------------------------------------

# Import system modules
import sys, os, arcpy, shutil, zipfile, glob, ckanclient, datetime, argparse
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
	
	parser = argparse.ArgumentParser(fromfile_prefix_chars='@')
			
	# Optional arguments	   
	parser.add_argument('-o', '--output-folder',
		action='store', 
		dest='output_folder',
		help='The root output folder in which to create the published files.  Sub-folders will automatically be created for each dataset (ex. C:\\\\temp).')
	
	parser.add_argument('-d', '--download-url',
		action='store', 
		dest='download_url',
		help='The root path to the data download repository. (ex. (ex. http://data.denvergov.org/)).')
	
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
	
	parser.add_argument('-p', '--ckan-dataset-name-prefix',
		action='store', 
		dest='ckan_dataset_name_prefix',
		default='',
		help='A prefix used in conjunction with the dataset-name argument to create the complete dataset name on OpenColorado.')

	parser.add_argument('-t', '--ckan-dataset-title-prefix',
		action='store', 
		dest='ckan_dataset_title_prefix',
		default='',
		help='A prefix used in conjunction with the dataset-title argument to create the complete dataset title on OpenColorado.')
	
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
		
	parser.add_argument('dataset_name',
		action='store',
		help='The name of the dataset on OpenColorado.  If a prefix is provided (-p) don''t include it here.')

	parser.add_argument('dataset_title',
		action='store',
		help='The title of the dataset on OpenColorado.  If a prefix is provided (-t) don''t include it here.')
			
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
	debug(' Dataset name: ' + args.dataset_name)
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

def publish_file(directory, file_name, file_type):
	"""Publishes a file to the catalog download folder
	
	Returns:
        None
	"""
	
	folder = create_folder(os.path.join(output_folder,get_dataset_filename()))
	
	folder = create_folder(os.path.join(folder,file_type))
	
	info(' Copying ' + file_name + ' to ' + folder)
	shutil.copyfile(os.path.join(directory,file_name), os.path.join(folder,file_name))

def get_dataset_filename():
	"""Gets a file system friendly name from the catalog dataset name
	
	Returns:
        None
	"""
	global args
	return args.dataset_name.replace("-","_")
	
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
	zip_file = zipfile.ZipFile(os.path.join(working_folder,name + ".zip"), "w")
	
	for filename in glob.glob(working_folder + "/" + name + "/*"):
		zip_file.write(filename, os.path.basename(filename), zipfile.ZIP_DEFLATED)
		
	zip_file.close()
	
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
	"""Updates the dataset version number on CKAN repository
	
	Returns:
        None
	"""	
	global args
	
	# Initialize ckan client
	ckan = ckanclient.CkanClient(base_location=args.ckan_api,api_key=args.ckan_api_key)
	
	# Create the name of the dataset on the CKAN instance
	dataset_id = args.ckan_dataset_name_prefix + args.dataset_name
	
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
	"""Updates the ckan dataset entity by reading in metadata
	   from the ArcGIS Metadata xml file. If the dataset already
	   exists in the CKAN repository, the dataset is fecthed
	   and modified. If the dataset does not already exist,
	   a new dataset entity object is created. 
	
	Returns:
        An object structured the same as the JSON dataset output from
        the CKAN REST API. For more information on the structure look at the
        web service JSON output, or reference:
        http://docs.ckan.org/en/latest/api-v2.html#model-api
	"""		
	global args
	
	folder = 'metadata'
	name = get_dataset_filename()
	
	# Initialize ckan client
	ckan = ckanclient.CkanClient(base_location=args.ckan_api,api_key=args.ckan_api_key)
	
	# Create the name of the dataset on the CKAN instance
	dataset_id = args.ckan_dataset_name_prefix + args.dataset_name

	try:
		# Get the dataset
		dataset_entity = ckan.package_entity_get(dataset_id)
		info(" Dataset " + dataset_id + " found on OpenColorado")
		
	except ckanclient.CkanApiNotFoundError:
		dataset_entity = None
		info(" Dataset " + dataset_id + " not found on OpenColorado")

	if dataset_entity is None:
		info(" New Dataset " + dataset_id + " being created on OpenColorado")
		dataset_entity = {};
	
	# Create a kml folder in the temp directory if it does not exist
	working_folder = os.path.join(output_folder,name,temp_folder,folder)
	create_folder(working_folder, True)
	
	# Set the destinion of the metadata export
	source = source_feature_class
	destination = os.path.join(working_folder,name + ".xml")
	
	# Export the metadata
	arcpy.env.workspace = "C:/temp"
	installDir = arcpy.GetInstallInfo("desktop")["InstallDir"]
	translator = installDir + "Metadata/Translator/ESRI_ISO2ISO19139.xml"
	
	arcpy.ExportMetadata_conversion(source, translator, destination)
	
	# Publish the metadata to the download folder
	publish_file(working_folder, name + ".xml","metadata")
	
	# Open the file and read in the xml
	metadata_file = open(destination,"r")
	metadata_xml = et.parse(metadata_file)

	# Specify the namespaces
	ns_gmd = "{http://www.isotc211.org/2005/gmd}"
	ns_gco = "{http://www.isotc211.org/2005/gco}"

	# Get the dataset title
	title = args.ckan_dataset_title_prefix + ": " + args.dataset_title
	
	# Get the dataset description
	xpath_description = '//{0}identificationInfo/{0}MD_DataIdentification/{0}citation/{0}CI_Citation/{0}title/{1}CharacterString'.format(ns_gmd,ns_gco);
	description = metadata_xml.find(xpath_description).text
	
	# Get the abstract
	xpath_abstract = '{0}identificationInfo/{0}MD_DataIdentification/{0}abstract/{1}CharacterString'.format(ns_gmd,ns_gco);
	abstract = metadata_xml.find(xpath_abstract).text
	debug ('Abstract from metadata: ' + abstract);
	
	# Get the keywords
	xpath_keywords = '//{0}MD_Keywords/{0}keyword/{1}CharacterString'.format(ns_gmd,ns_gco);
	keyword_elements = metadata_xml.findall(xpath_keywords)
	keywords = []
	for keyword_element in keyword_elements:
		keyword = keyword_element.text
		keyword = keyword.lower().replace(' ','-')
		keywords.append(keyword)
		debug ('Keywords found in metadata: ' + keyword);

	# Get the maintainer
	xpath_maintainer= '//{0}distributionInfo/{0}MD_Distribution/{0}distributor/{0}MD_Distributor/{0}distributorContact/{0}CI_ResponsibleParty/{0}organisationName/{1}CharacterString'.format(ns_gmd,ns_gco);
	maintainer = metadata_xml.find(xpath_maintainer).text

	# Get the maintainer email
	xpath_maintainer_email = '//{0}distributionInfo/{0}MD_Distribution/{0}distributor/{0}MD_Distributor/{0}distributorContact/{0}CI_ResponsibleParty/{0}contactInfo/{0}CI_Contact/{0}address/{0}CI_Address/{0}electronicMailAddress/{1}CharacterString'.format(ns_gmd,ns_gco);
	maintainer_email = metadata_xml.find(xpath_maintainer_email).text

	# Get the author
	xpath_author = '//{0}identificationInfo/{0}MD_DataIdentification/{0}pointOfContact/{0}CI_ResponsibleParty/{0}individualName/{1}CharacterString'.format(ns_gmd,ns_gco);
	author = metadata_xml.find(xpath_author).text

	# Get the author_email
	xpath_author_email = '//{0}identificationInfo/{0}MD_DataIdentification/{0}pointOfContact/{0}CI_ResponsibleParty/{0}contactInfo/{0}CI_Contact/{0}address/{0}CI_Address/{0}electronicMailAddress/{1}CharacterString'.format(ns_gmd,ns_gco);
	author_email = metadata_xml.find(xpath_author_email).text
	
	# Update the dataset attributes from the ArcGIS Metadata
	dataset_entity['title'] = title
	dataset_entity['notes'] = abstract
	dataset_entity['tags'] = keywords
	dataset_entity['maintainer'] = maintainer
	dataset_entity['maintainer_email'] = maintainer_email
	dataset_entity['author'] = author
	dataset_entity['author_email'] = author_email

	# Construct the file resource download urls   
	dataset_name = get_dataset_filename()   
	resources = [
		{
			'name': title + ' - KML',
			'description': description + ' - KML',
			'url': args.download_url + dataset_name + '/kml/' + dataset_name + '.kmz',
			'mimetype': 'application/vnd.google-earth.kmz',
			'format': 'KML'
		},
		{
			'name': title + ' - Shapefile',
			'description': description + ' - Shapefile',			
			'url': args.download_url + dataset_name + '/shape/' + dataset_name + '.zip',
			'mimetype': 'application/zip',			
			'format': 'SHP'
		},
		{
			'name': title + ' - AutoCAD DWG',
			'description': description + ' - AutoCAD DWG',
			'url': args.download_url + dataset_name + '/cad/' + dataset_name + '.dwg',
			'mimetype': 'application/acad',
			'format': 'DWG'
		}
	]
		
	dataset_entity['resources'] = resources;
	
	# Update the CKAN groups the dataset belongs to
	group_list = ckan.group_register_get()
	dataset_entity['groups'] = group_list
	
	if dataset_entity.has_key('name'):
		# Update the dataset
		ckan.package_entity_put(dataset_entity)
	else:
		# This dataset doesn't exist on OpenColorado, create new dataset 
		dataset_entity['name'] = dataset_id
		dataset_entity['license_id'] = 'cc-zero'
		ckan.package_register_post(dataset_entity)	

def increment_version(version, increment_type):
	"""Increments the version number
	
	Parameters:
		version - A version in the format <major.minor.revision>
		increment_type - [major, minor, or revision]
	
	Returns:
        a string representing the incremented version in the format
        <major.minor.revision>
	"""
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
	"""Replaces <Null> with empty string in data fields
	
	Parameters:
		feature_layer - The name of the ArcGIS dataset to replace values in
	
	Returns:
        None
	"""
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
	
