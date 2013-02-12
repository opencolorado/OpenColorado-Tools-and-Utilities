# ---------------------------------------------------------------------------
#
# opencolorado_specific_shapefile_download.py
#
# ---------------------------------------------------------------------------
# Downloads and reprojects all shapefiles from Open Colorado
## Destination SRID = 2232 (NAD83 / Colorado Central (ftUS)...)
## You can change the destination SRID in the reproject_shapefile method below.
#----------------------------------------------------------------------------
# This script completes the following:
# 1) Accesses a CKAN instance and downloads the datasets having the name you specify.
# 2) Extracts the shapefile and re-projects it to 2232
#----------------------------------------------------------------------------

####  need to make it possible to set target download location.

import os, sys, urllib2, zipfile, shutil
import ckanclient
from osgeo import ogr, osr


# Globals
download_folder = "download"

ckan_host = "http://data.opencolorado.org/api/2"


def main():
    
    print "Running a single shapefile download script!"

    initialize()
    
    #### Change the name below to the desired target data set for download.
    process_ckan_datasets('9f7cea9e-3814-4d1f-a165-9b2db9b4b380')
    
    print "Process complete!"

def initialize():

    global download_folder
    
    # Create the download folder if it does not exist
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)
      
def process_ckan_datasets(package_id):
    
    global ckan_host
    
    # Initialize the CKAN client  
    ckan_client = ckanclient.CkanClient(base_location=ckan_host)
    
    package_id_list = ckan_client.package_register_get()
   # print package_id_list  
    
    # Get the package details
    package = ckan_client.package_entity_get(package_id)

            
    # Get the package name (slug)
    package_name = package['name']
    
    print "------------------------------"
    print "Processing dataset: " + package_name
    print "Created: " + package['metadata_created'] + ", modified: " + package['metadata_modified']
    
    shapefile_found = False
    resources = package['resources']
    for resource in resources:
        ## Look for a shapefile resource
        if (resource['mimetype'] and 'shp' in resource['mimetype'].lower()) or \
           (resource['mimetype_inner'] and 'shp' in resource['mimetype_inner'].lower()) or \
           (resource['format'] and 'shp' in resource['format'].lower()) or \
           (resource['format'] and 'shapefile' in resource['format'].lower()) or \
           (resource['name'] and 'shp' in resource['name'].lower()) or \
           (resource['name'] and 'shapefile' in resource['name'].lower()) or \
           (resource['description'] and 'shp' in resource['description'].lower()) or \
           (resource['description'] and 'shapefile' in resource['description'].lower()):
            
            shapefile_found = True
       
            print "Shapefile found!  Attepting download..."
            
            # Get the resource URL
            url = resource["url"]
            
            #### Download the shapefile
            shapefile = download_shapefile(package_name,url)
            
            reproject_shapefile(package_name, shapefile)
        else:
            pass
    
    if shapefile_found == False:
        print "No shapefile found."
                                        
    
def download_shapefile(package_name,url):
    global download_folder
    
    shapefile = None
    
    dataset_download_folder = os.path.join(download_folder)
    dataset_download_folder_source = os.path.join(dataset_download_folder,"source")
    
    dataset_download_file = os.path.join(dataset_download_folder, "shapefile.zip")
    
    try:
        print "Downloading.."
        request = urllib2.urlopen(url)
        with open(dataset_download_file, 'wb') as fp:
            shutil.copyfileobj(request, fp)
        
        # Unzip the file
        print "Unzipping.."
        zip = zipfile.ZipFile(dataset_download_file)
        zip.extractall(dataset_download_folder_source)
        zip.close()
    
        # Delete the zip file
        os.remove(dataset_download_file)
        
        # Find the shapefile (if this zip actually contains a shapefile)
        for (path, dirs, files) in os.walk(dataset_download_folder_source):
            for file in files:
                if file.endswith(".shp"):
                    shapefile = os.path.join(path,file)
    except:
        print "***Error downloading file: "+ url
        
    return shapefile

def reproject_shapefile(package_name, shapefile):
    
    global download_folder
    
    print "Reprojecting shapefile..."
    
    source_folder = os.path.join(download_folder,"source")
    projected_folder = os.path.join(download_folder,"projected")
    projected_shapefile = os.path.join(projected_folder, package_name+".shp")
    projected_shapefile_prj = os.path.join(projected_folder, package_name+".prj")
    
    if not os.path.exists(projected_folder):
        os.makedirs(projected_folder)
        
    driver = ogr.GetDriverByName('ESRI Shapefile')
     
    src_shapefile = ogr.Open(encode_path(shapefile))
    
    src_layer = src_shapefile.GetLayer()
    
    src_geom_type = src_layer.GetLayerDefn().GetGeomType()
    
    # Get the input SpatialReference
    src_sr = src_layer.GetSpatialRef()

    # create the output SpatialReference
    ## Change the dest_sr value below to the desired destination srid.
    dest_sr = osr.SpatialReference()
    dest_sr.ImportFromEPSG(2232)
    
    # create the CoordinateTransformation
    transformation = osr.CoordinateTransformation(src_sr, dest_sr)
    
    # create a new data source and layer
    if os.path.exists(projected_shapefile):
        driver.DeleteDataSource(encode_path(projected_shapefile))
      
    dest_shapefile = driver.CreateDataSource(encode_path(projected_shapefile))
    
    if dest_shapefile is None:
        print 'Could not create file'
        sys.exit(1)
        
    dest_layer = dest_shapefile.CreateLayer('output', geom_type=src_geom_type)
    
    # get the layer definition for the output shapefile
    dest_layer_defn = dest_layer.GetLayerDefn()
    
    # Get the first source feature
    src_feature = src_layer.GetNextFeature()
    
    while src_feature:
    
        # get the input geometry
        src_geom = src_feature.GetGeometryRef()
        
        if (src_geom != None): 
            # reproject the geometry
            src_geom.Transform(transformation)
            
            # create a new feature
            dest_feature = ogr.Feature(dest_layer_defn)
            
            # set the geometry and attribute
            dest_feature.SetGeometry(src_geom)
            
            # add the feature to the shapefile
            dest_layer.CreateFeature(dest_feature)
            
            # destroy the features and get the next input feature
            dest_feature.Destroy
            src_feature.Destroy
            
            src_feature = src_layer.GetNextFeature()
        else:
            projected_shapefile = None
            print "Unable to load source geometry"
            break
    
    # close the shapefiles
    dest_shapefile.Destroy()
    src_shapefile.Destroy()
    
    # Delete the source shapefile
    try:
        shutil.rmtree(source_folder)
    except:
        print "Unable to delete the source shapefile (" + source_folder + ").  Skipping..."
        
    # create the *.prj file
    ## Change the OGC WKT value below to match your desired destination srid (ex. http://spatialreference.org/ref/epsg/2232/ogcwkt/)
    if projected_shapefile != None:
        file = open(projected_shapefile_prj, 'w')
        file.write('PROJCS["NAD83 / Colorado Central (ftUS)",GEOGCS["NAD83",DATUM["North_American_Datum_1983",SPHEROID["GRS 1980",6378137,298.257222101,AUTHORITY["EPSG","7019"]],AUTHORITY["EPSG","6269"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.01745329251994328,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4269"]],UNIT["US survey foot",0.3048006096012192,AUTHORITY["EPSG","9003"]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["standard_parallel_1",39.75],PARAMETER["standard_parallel_2",38.45],PARAMETER["latitude_of_origin",37.83333333333334],PARAMETER["central_meridian",-105.5],PARAMETER["false_easting",3000000],PARAMETER["false_northing",1000000],AUTHORITY["EPSG","2232"],AXIS["X",EAST],AXIS["Y",NORTH]]')
        file.close()
    
    return projected_shapefile


def encode_path(path):
    filesystemencoding = sys.getfilesystemencoding()
    return path.encode(filesystemencoding)
    
    
#Execute main function    
if __name__ == '__main__':
    main()
