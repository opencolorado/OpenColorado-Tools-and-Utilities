# ---------------------------------------------------------------------------
# DrcogSync.py
# ---------------------------------------------------------------------------
# Synchronize datasets from the DRCOG Regional Data Catalog to OpenColorado
#----------------------------------------------------------------------------

# Imports
import os
import sys
import urllib2
import ckanclient
import logging
import time
from bs4 import BeautifulSoup

# Global variables
base_url = "http://gis.drcog.org"
data_catalog_prefix = "/datacatalog"
subjects_url_prefix = "/datacatalog/subjects/"
dataset_url_prefix = "/datacatalog/content/"
ckan_client = None

ckan_host = "http://data.opencolorado.org/api/2"
ckan_key = sys.argv[1]
ckan_group = "drcog"
ckan_title_prefix = "DRCOG: "
ckan_name_prefix = "drcog-"
ckan_license = "cc-by"

def main():
    
    localtime = time.asctime( time.localtime(time.time())) 
    print "-----------------------------------------------------"
    print str(localtime) + " - starting synchronization"
    print "-----------------------------------------------------"
    
    # Get the current list of CKAN datasets on OpenColorado
    ckan_datasets = get_ckan_datasets()
    
    # Get the current list of datasets on DRCOG
    drcog_datasets = get_drcog_datasets()

    # Remove datasets from CKAN that are no longer provided by DRCOG
    delete_removed_datasets(drcog_datasets, ckan_datasets)
    
    # Begin syncing each dataset to CKAN
    print "Syncing DRCOG datasets to OpenColorado"
    for drcog_dataset in drcog_datasets:
        print "------------------------------------------------------------------"
        print "Dataset: " + drcog_dataset
        drcog_dataset_entity = get_dataset_entity(drcog_dataset)
        publish_to_ckan(drcog_dataset_entity)
        
        #break #Just do the first dataset for now

    localtime = time.asctime( time.localtime(time.time())) 
    print "-----------------------------------------------------"
    print str(localtime) + " - Synchronization complete"
    print "-----------------------------------------------------"
    
def retry(ExceptionToCheck, tries=3, delay=3, backoff=2, logger=None):
    """Retry calling the decorated function using an exponential backoff.

    http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
    original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

    :param ExceptionToCheck: the exception to check. may be a tuple of
        exceptions to check
    :type ExceptionToCheck: Exception or tuple
    :param tries: number of times to try (not retry) before giving up
    :type tries: int
    :param delay: initial delay between retries in seconds
    :type delay: int
    :param backoff: backoff multiplier e.g. value of 2 will double the delay
        each retry
    :type backoff: int
    :param logger: logger to use. If None, print
    :type logger: logging.Logger instance
    """
    def deco_retry(f):
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            try_one_last_time = True
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                    try_one_last_time = False
                    break
                except ExceptionToCheck, e:
                    msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                    if logger:
                        logger.warning(msg)
                    else:
                        print msg
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            if try_one_last_time:
                return f(*args, **kwargs)
            return
        return f_retry  # true decorator
    return deco_retry

@retry(Exception)
def get_ckan_datasets():
    """Gets the current DCROG datasets on OpenColorado
        
    Returns:
        None
    """
    global ckan_group, ckan_client, ckan_host, ckan_key
    
    print "Getting DRCOG datasets from OpenColorado"
    
    # Initialize the CKAN client  
    ckan_client = ckanclient.CkanClient(base_location=ckan_host,api_key=ckan_key)
                                        
    results = ckan_client.package_search(None, search_options={'groups': ckan_group, 'all_fields': 1, 'limit': 5000})
        
    datasets = list(results["results"])
    
    print str(len(datasets)) + " datasets found"
    
    return datasets    

def get_drcog_datasets():
    """Gets the current datasets on DRCOG
        
    Returns:
        List of datasets
    """
    # Get a list of subjects from the DRCOG catalog
    datasets = []
    subjects = get_subjects()
    
    # Get all datasets by subject (datasets may be in more than one subject so there could be duplicates here)
    print "Getting datasets from DRCOG data catalog"
    for subject in subjects:
        sys.stdout.write('.')
        datasets = datasets + get_dataset_urls_by_subject(subject)
        #break # Just do the first subject for now
    
    print ""
    
    # Remove duplicates
    datasets = list(set(datasets))
    
    print str(len(datasets)) + " datasets found"
    
    return datasets

def delete_removed_datasets(drcog_datasets,ckan_datasets):
    """Removes datasets from CKAN that are no longer published by DRCOG

    Parameters:
        drcog_datasets - The list of datasets currently provided by DRCOG
        ckan_datasets - The list of datasets from DRCOG currently on OpenColorado
    
    Returns:
        None
    """   
    global ckan_name_prefix
    
    datasets_to_remove = []
    
    print "Checking datasets to remove from OpenColorado"
    for ckan_dataset in ckan_datasets:
        ckan_dataset_name = ckan_dataset["name"]
        found = False
        
        for drcog_dataset in drcog_datasets:
            drcog_dataset_name = ckan_name_prefix + drcog_dataset
            
            if (drcog_dataset_name == ckan_dataset_name):
                found = True
                break
        
        if (found == False):
            datasets_to_remove.append(ckan_dataset_name)
    
    print str(len(datasets_to_remove)) + " marked for deletion from OpenColorado:"
    for dataset_to_remove in datasets_to_remove:
        print "  -" + dataset_to_remove
    
    # Delete the datasets
    for dataset_to_remove in datasets_to_remove:
        delete_ckan_dataset(dataset_to_remove)
        
@retry(Exception)
def delete_ckan_dataset(name):
    
    global ckan_client, ckan_host, ckan_key
    
    # Initialize the CKAN client  
    ckan_client = ckanclient.CkanClient(base_location=ckan_host,api_key=ckan_key)
            
    print "  Deleting CKAN dataset " + name                            
    results = ckan_client.package_entity_delete(name)
        
    
@retry(Exception)        
def get_subjects():
    """Gets a list of subjects from the DRCOG data catalog

    Returns:
        List[string]
    """
    global base_url, subjects_url_prefix
    
    print "Getting list of subjects from DRCOG data catalog"
    
    subjects = []
    subjects_url = base_url + "/datacatalog/content/welcome-regional-data-catalog?quicktabs_tabbed_menu_homepage=1"
    
    soup = get_soup_from_url(subjects_url)

    for link in soup.find_all('a'):
        href = link.get('href')
        if href.startswith(subjects_url_prefix):
            subject = href.replace(subjects_url_prefix,"")
            subjects.append(subject)
    
    print "Retrieved subjects"
    
    return subjects

@retry(Exception)
def get_dataset_urls_by_subject(subject, page_url=None):
    global base_url, subjects_url_prefix, dataset_url_prefix
    
    datasets = []
        
    datasets_url = base_url + subjects_url_prefix + subject
    
    if page_url != None:
        datasets_url = base_url + page_url
        
    #print datasets_url
    
    soup = get_soup_from_url(datasets_url)
    
    #print(soup.prettify())
    
    # Get all of the dataset links on the current page
    for div in soup.findAll("div", { "class" : "node" }):
        if div.h2 != None and div.h2.a != None:
            href = div.h2.a.get('href')
            if href.startswith(dataset_url_prefix):
                dataset = href.replace(dataset_url_prefix,"")
                datasets.append(dataset)
                #print("-- " + dataset)
        
    # If there is a next link, recursively get the next page of datasets
    pager_next = soup.find("li", { "class" : "pager-next" })
    
    if pager_next != None:
        next_href = pager_next.a.get('href')
        datasets = datasets + get_dataset_urls_by_subject(subject,next_href)
    
    return datasets

@retry(Exception)
def get_dataset_entity(dataset):
    global base_url, dataset_url_prefix, ckan_title_prefix, ckan_name_prefix, ckan_license, data_catalog_prefix

    dataset_url = base_url + dataset_url_prefix + dataset
    
    dataset_entity = {}
        
    soup = get_soup_from_url(dataset_url)
    
    # Scrape the content from the dataset page
    dataset_entity['name'] = ckan_name_prefix + dataset
    dataset_entity['title'] = ckan_title_prefix + soup.find("h1",{ "id" : "page-title" }).getText()
    dataset_entity['license_id'] = ckan_license
    dataset_entity['url'] = dataset_url   
        
    resources = []
 
    # Get the tags
    tags = []
    terms_element = soup.find("div", { "class" : "terms"})
    if (terms_element != None) :
        for li in terms_element.findAll("li"):
            tag = li.getText()
            tag = tag.lower().replace(' ','-')
            tag = tag.lower().replace('(','')
            tag = tag.lower().replace(')','')
            tags.append(tag)
    dataset_entity['tags'] = tags
                
        
    # Get descriptive information
    for field_item in soup.findAll("div", { "class" : "field-item" }):
        
        # Get the field label (if it exists)
        field_item_label = ""
        field_item_div = field_item.div
        if (field_item_div != None):
            field_item_label = field_item_div.getText().strip().lower()
            if (len(field_item_label) > 0 and not field_item_label.endswith(".pdf")):
                field_item.div.extract() # Remove the label tag so we can get the remaining text by itself
        
        field_item_link = field_item.a
        field_item_text = field_item.getText().strip()
        
        #print field_item_label
        #print field_item_link
        #print field_item_text
    
        # Get dataset attributes    
        if (field_item_label.startswith("description:")):
            dataset_entity['notes'] = field_item_text
        elif (field_item_label.startswith("source:")):
            dataset_entity['author'] = field_item_text
        elif (field_item_label.startswith("contact name:")):
            dataset_entity['maintainer'] = field_item_text
        elif (field_item_label.startswith("contact email:")):
            dataset_entity['maintainer_email'] = field_item_text
    
        # Get resources
        if (field_item_link != None):
            resource_url = field_item_link.get('href')
            
            resource = {}
            
            # If paths are relative make them absolute
            if (resource_url.startswith("http")):
                resource["url"] = resource_url
            else:
                resource["url"] = base_url + resource_url
                
            resource["name"] = dataset_entity['title']
            
            if (field_item_label.startswith("kml")):
                resource["format"] = "KML"
                resource["name"] = dataset_entity['title'] + " - KML"
                resource["mimetype"] = "application/vnd.google-earth.kmz"
            elif (field_item_label.startswith("wms")):
                resource["format"] = "WMS"
                resource["name"] = dataset_entity['title'] + " - WMS"
                resource["mimetype"] = "application/wms"
            elif (field_item_label.startswith("georss")):
                resource["format"] = "RSS"
                resource["name"] = dataset_entity['title'] + " - GeoRSS"
                resource["mimetype"] = "application/rss"
            elif (field_item_label.startswith("shapefile")):
                resource["format"] = "SHP"
                resource["name"] = dataset_entity['title'] + " - SHP"
                resource["mimetype"] = "application/zip"
            
            if ("mimetype" in resource):
                resources.append(resource)
            
    # Search for other filefields (PDF) 
    filefield_file = soup.find("div", { "class" : "filefield-file" })
    if (filefield_file != None):
        filefield_file_label = filefield_file.a.getText().strip()
        filefield_file_link = filefield_file.a.get('href')
        filefield_file_mimetype = filefield_file.a.get('type')
        
        if (filefield_file_mimetype.startswith("application/pdf")) :            
            resource = {}
            if (filefield_file_link.startswith("http")):
                resource["url"] = filefield_file_link
            else:
                resource["url"] = base_url + filefield_file_link
                
            resource["name"] = filefield_file_label
            resource["format"] = "PDF"
            resource["mimetype"] = "application/pdf" #Hack, need to get this from URL?    
            resources.append(resource) 

    # Add the resources to the dataset
    dataset_entity["resources"] = resources
    
    print "  Retrieved dataset details from DRCOG catalog" 
    print "    Name: " +  dataset_entity['name']
    print "    Title: " +  dataset_entity['title']
    print "    Tags:" + str(dataset_entity['tags'])
    print "    Resources:"
    for resource in dataset_entity["resources"]:
        print "      " + resource["format"] + " (" + resource["mimetype"] + "): " + resource["url"]  
    print ""
    
    return dataset_entity

@retry(Exception)
def publish_to_ckan(dataset_entity):
    """Updates the dataset in the CKAN repository or creates a new dataset

    Returns:
        None
    """
    global ckan_client, ckan_host, ckan_key
    
    print "Publishing dataset to CKAN"
    
    # Initialize the CKAN client  
    ckan_client = ckanclient.CkanClient(base_location=ckan_host,api_key=ckan_key)
    
    # Create the name of the dataset on the CKAN instance
    dataset_id = dataset_entity["name"]
    
    # Get the dataset from CKAN
    dataset_entity_remote = get_remote_dataset(dataset_id)
    
    # Check to see if the dataset exists on CKAN or not
    if dataset_entity_remote is None:
        # Create a new dataset
        create_dataset(dataset_entity)
    else:
        # Update an existing dataset
        update_dataset(dataset_entity_remote, dataset_entity)

@retry(Exception)
def create_dataset(dataset_entity):
    """Creates a new dataset and registers it to CKAN

    Parameters:
        dataset_entity - A CKAN dataset entity
    
    Returns:
        None
    """       
    global ckan_group
    
    try:
        group_entity = ckan_client.group_entity_get(ckan_group)
        if group_entity is not None:
            print('  Adding dataset to group: ' + ckan_group)      
            dataset_entity['groups'] = [group_entity['id']]
    except ckanclient.CkanApiNotFoundError:
        dataset_entity['groups'] = []     
     
    ckan_client.package_register_post(dataset_entity)
    
@retry(Exception)
def update_dataset(dataset_entity_remote, dataset_entity):
    """Updates a dataset

    Parameters:
        dataset_entity_remote - The target dataset
        dataset_entity - The updated dataset
    
    Returns:
        None
    """    
    dataset_entity_remote['url'] = dataset_entity["url"]
    dataset_entity_remote['license_id'] = dataset_entity["license_id"]
    dataset_entity_remote['name'] = dataset_entity["name"]
    dataset_entity_remote['title'] = dataset_entity["title"]
    dataset_entity_remote['notes'] = dataset_entity["notes"]
    dataset_entity_remote['author'] = dataset_entity["author"]
    dataset_entity_remote['maintainer'] = dataset_entity["maintainer"]
    dataset_entity_remote['maintainer_email'] = dataset_entity["maintainer_email"]
    dataset_entity_remote['tags'] = dataset_entity["tags"]
    
    # Process resources
    if not 'resources' in dataset_entity_remote:
       dataset_entity_remote['resources'] = []
        

    print "    Updating resources:"
    for resource in dataset_entity['resources']:
        
        mimetype = ""
        if 'mimetype' in resource:
            mimetype = resource['mimetype']
        
        # Check if a resource is already present with the same mimetype
        # If so, update it, otherwise add as a new resource
        found = False
        for resource_remote in dataset_entity_remote['resources']:
            if 'mimetype' in resource_remote and resource_remote['mimetype'] == mimetype:
                found = True
                break
        
        if (found) :
            print "      Resource found (" + mimetype + ").  Updating..."
            resource_remote["url"] = resource["url"]
            resource_remote["name"] = resource["name"]
            resource_remote["format"] = resource["format"]
            resource_remote["mimetype"] = resource["mimetype"]
        else:
            print "      Resource not found (" + mimetype + ").  Adding..."
            dataset_entity_remote['resources'].append(resource)
            
    ckan_client.package_entity_put(dataset_entity_remote)

@retry(Exception)
def get_remote_dataset(dataset_id):
    """Gets the dataset from CKAN repository

    Parameters:
        dataset_id - A string representing the unique dataset name
    
    Returns:
        An object structured the same as the JSON dataset output from
        the CKAN REST API. For more information on the structure look at the
        web service JSON output, or reference:
        http://docs.ckan.org/en/latest/api-v2.html#model-api
    """
    dataset_entity = None

    try:
        # Get the dataset
        dataset_entity = ckan_client.package_entity_get(dataset_id)
        print("  Dataset found on OpenColorado")
        
    except ckanclient.CkanApiNotFoundError:
        print("  Dataset not found on OpenColorado")

    return dataset_entity

def get_soup_from_url(url):
    html = "".join(urllib2.urlopen(url).readlines())
    soup = BeautifulSoup(html)
    return soup

#Execute main function    
if __name__ == '__main__':
    main()