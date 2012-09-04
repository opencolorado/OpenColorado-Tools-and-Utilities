# ---------------------------------------------------------------------------
# analysis.py
# ---------------------------------------------------------------------------
# Generate a heat map from shapefiles in a CKAN data catalog
#----------------------------------------------------------------------------
import random, os, sys, Image, ImageFilter, urllib2, zipfile, shutil
import ckanclient
from osgeo import gdal, ogr, osr
#from numpy import *

# Globals
datasets_folder = "datasets"
download_folder = "download"

ckan_host = "http://data.opencolorado.org/api/2"

x_min = -12140532.1637
x_max = -11359138.5791
y_min = 4438050.84302
y_max = 5012849.66619
resolution = 75 # Pixel size in web mercator meters

def main():
    
    print "Running analysis.py"
    
    #initialize()
    
    #process_ckan_datasets()
    
    generate_composite_image()
    
    print "Done"

def initialize():
    
    global datasets_folder
    
    if not os.path.exists(datasets_folder):
        os.makedirs(datasets_folder)
    
def process_ckan_datasets():
    
    global ckan_host
    
    # Initialize the CKAN client  
    ckan_client = ckanclient.CkanClient(base_location=ckan_host)
    
    package_id_list = ckan_client.package_register_get()
    
    index = 0;
    for package_id in package_id_list:
        
        if index >=141 and index <= 500:
            
            # Get the package details
            package = ckan_client.package_entity_get(package_id)
            
            # Get the package name (slug)
            package_name = package['name']
            
            dataset_folder = os.path.join(datasets_folder,package_name)
            
            print "------------------------------"
            print "Processing dataset " + str(index) + " of " + str(len(package_id_list)) + ": " + package_name
            
            resources = package['resources']
            for resource in resources:
                
                # Look for a shapefile resource
                if (resource['mimetype'] and 'shp' in resource['mimetype'].lower()) or \
                   (resource['mimetype_inner'] and 'shp' in resource['mimetype_inner'].lower()) or \
                   (resource['format'] and 'shp' in resource['format'].lower()) or \
                   (resource['format'] and 'shapefile' in resource['format'].lower()) or \
                   (resource['name'] and 'shp' in resource['name'].lower()) or \
                   (resource['name'] and 'shapefile' in resource['name'].lower()) or \
                   (resource['description'] and 'shp' in resource['description'].lower()) or \
                   (resource['description'] and 'shapefile' in resource['description'].lower()):
                    
                    # Get the download URL
                    url = resource["url"]
                    
                    # Check if the map image exists for this dataset before downloading again
                    # TODO: Use timestamp of dataset and image
                    if not os.path.exists(os.path.join(dataset_folder,"map.png")):
                    
                        # Download the shapefile
                        shapefile = download_shapefile(package_name, url)
                        
                        # Process the shapefile
                        if (shapefile != None):
                            process_shapefile(package_name, shapefile)
                    else:
                        print "Map image exists, skipping dataset.."
                        
        index = index + 1    

def initialize_dataset_folder(package_name):
    global datasets_folder, download_folder
    
    dataset_folder = os.path.join(datasets_folder,package_name)
    dataset_download_folder = os.path.join(dataset_folder, download_folder)
    
    if not os.path.exists(dataset_folder):
        os.makedirs(dataset_folder)
        
    if not os.path.exists(dataset_download_folder):
        os.makedirs(dataset_download_folder)
    
    return dataset_folder
        
def download_shapefile(package_name,url):
    global download_folder
    
    shapefile = None
    
    # Initialize the folder to hold the dataset
    dataset_folder = initialize_dataset_folder(package_name)
    
    dataset_download_folder = os.path.join(dataset_folder, download_folder)
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
        for file in os.listdir(dataset_download_folder_source):
            if file.endswith(".shp"):
                shapefile = os.path.join(dataset_download_folder_source,file)
    except:
        print "Error downloading file: "+ url
        
    return shapefile
    
def process_shapefile(package_name, shapefile):
    
    projected_shapefile = reproject_shapefile(package_name, shapefile)
    
    if (projected_shapefile != None):
        rasterize_shapefile(package_name, projected_shapefile)
    

def reproject_shapefile(package_name, shapefile):
    
    global download_folder
    
    print "Reprojecting shapefile..."
    
    dataset_folder = initialize_dataset_folder(package_name)
    dataset_download_folder = os.path.join(dataset_folder, download_folder)
    
    source_folder = os.path.join(dataset_download_folder,"source")
    projected_folder = os.path.join(dataset_download_folder,"projected")
    projected_shapefile = os.path.join(projected_folder, "projected.shp")
    projected_shapefile_prj = os.path.join(projected_folder, "projected.prj")
    
    if not os.path.exists(projected_folder):
        os.makedirs(projected_folder)
        
    driver = ogr.GetDriverByName('ESRI Shapefile')
     
    src_shapefile = ogr.Open(shapefile)
    
    src_layer = src_shapefile.GetLayer()
    
    # Get the input SpatialReference
    src_sr = src_layer.GetSpatialRef()

    # create the output SpatialReference
    dest_sr = osr.SpatialReference()
    dest_sr.ImportFromEPSG(3857)
    
    # create the CoordinateTransformation
    transformation = osr.CoordinateTransformation(src_sr, dest_sr)
    
    # create a new data source and layer
    if os.path.exists(projected_shapefile):
        driver.DeleteDataSource(projected_shapefile)
      
    dest_shapefile = driver.CreateDataSource(projected_shapefile)
    
    if dest_shapefile is None:
        print 'Could not create file'
        sys.exit(1)
        
    dest_layer = dest_shapefile.CreateLayer('output', geom_type=src_layer.GetGeomType())
    
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
    shutil.rmtree(source_folder)
    
    # create the *.prj file
    if projected_shapefile != None:
        file = open(projected_shapefile_prj, 'w')
        file.write('PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",GEOGCS["GCS_WGS_1984",DATUM["D_WGS_1984",SPHEROID["WGS_1984",6378137.0,298.257223563]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator_Auxiliary_Sphere"],PARAMETER["False_Easting",0.0],PARAMETER["False_Northing",0.0],PARAMETER["Central_Meridian",0.0],PARAMETER["Standard_Parallel_1",0.0],PARAMETER["Auxiliary_Sphere_Type",0.0],UNIT["Meter",1.0]]')
        file.close()  
    
    return projected_shapefile

def rasterize_shapefile(package_name, shapefile):
    """Rasterizes a shapefile to be used in building the composite
    heat map
    """
    global resolution, x_min, x_max, y_min, y_max
    
    print "Rasterizing... "
                 
    projected_shapefile = ogr.Open(shapefile)
    
    layer = projected_shapefile.GetLayer()
        
    # Get the spatial reference
    srs = layer.GetSpatialRef()
            
    # Create a field in the source layer to hold the features colors
    #field_def = ogr.FieldDefn(RASTERIZE_COLOR_FIELD, ogr.OFTReal)
    #layer.CreateField(field_def)
    #layer_def = layer.GetLayerDefn()
    
    #field_index = layer_def.GetFieldIndex(RASTERIZE_COLOR_FIELD)
    
    # Generate random values for the color field (it's here that the value
    # of the attribute should be used, but you get the idea)
    #for feature in layer:
    #    feature.SetField(field_index, random.randint(0, 255))
    #   layer.SetFeature(feature)
        
    # Create the destination data source
    x_res = int((x_max - x_min) / resolution)
    y_res = int((y_max - y_min) / resolution)
        
    output = os.path.join(datasets_folder, package_name, 'map.tif')
    output_png = os.path.join(datasets_folder, package_name, 'map.png')

    target_ds = gdal.GetDriverByName('GTiff').Create(output, x_res, y_res, 3, gdal.GDT_Byte)
    
    target_ds.SetGeoTransform((
            x_min, resolution, 0,
            y_max, 0, -resolution
      ))
    
    # Set the project of the target data source
    if srs:
        # Make the target raster have the same projection as the source
        target_ds.SetProjection(srs.ExportToWkt())
        
    # Rasterize the layer
    #options=["ATTRIBUTE=%s" % RASTERIZE_COLOR_FIELD]
    
    # Set the burn value based on the geometry type
    geom_type=layer.GetGeomType()
    if (geom_type == ogr.wkbPolygon):
        burn_value = 128
    else:
        burn_value = 255
    
    err = gdal.RasterizeLayer(target_ds, (1, 2, 3), layer, burn_values=(burn_value, burn_value, burn_value), options=[])
    if err != 0:
        raise Exception("error rasterizing layer: %s" % err)

    target_ds.FlushCache()
    target_ds = None
    
    # Open the tif image
    im = Image.open(output)
    
    # Convert to 8 bit
    im = im.convert("L")
    
    # If the type is not polygon, apply some smoothing
    if (geom_type != ogr.wkbPolygon):
        im = im.filter(ImageFilter.SMOOTH)
    
    # Save to a PNG image
    im.save(output_png)
    
    print "Created map image for dataset: " + package_name
    
    # Delete the TIF
    os.remove(output)

def generate_composite_image():
    
    global datasets_folder
    
    image_count = 0
    base_image = None
    
    print "------------------------------"
    print "Generating composite image"
    
    max = 255
    alpha_divisor = float(1)
    
    # Find the image maps for each dataset
    for folder in os.listdir(datasets_folder):
        for file in os.listdir(os.path.join(datasets_folder,folder)):
            if file == "map.png":
                
                alpha = float(alpha_divisor) / (image_count + 1)
                
                print "Blending image (alpha: " alpha + ") "+ str(image_count + 1) + ": " + folder
                
                image = os.path.join(datasets_folder,folder,file)
                
                # Use the first image as the base image
                if image_count == 0:
                    base_image = Image.open(image)
                else:
                    
                    # Get the maximum value in the base image
                    max = base_image.convert('L').getextrema()
                    
                    # Average the image into the base image
                    current_image = Image.open(image)
                    
                    # Compute the new alpha
                    
                    #print alpha
                    
                    Image.blend(base_image,current_image,alpha).save('map.png')
                    
                    
                                        
                    base_image = Image.open('map.png')
                
                image_count = image_count + 1
    
        if image_count > 100:
            break
        
    print "Composite image complete"            
        
    # Merge all of the files together
        
        #if os.path.isdir(folder):
        #    #if file.endswith(".shp"):
        #    #    shapefile = os.path.join(dataset_download_folder,file)
        #    print "whee" + folder
    
#Execute main function    
if __name__ == '__main__':
    main()