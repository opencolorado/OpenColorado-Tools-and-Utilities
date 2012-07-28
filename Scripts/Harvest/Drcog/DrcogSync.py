# ---------------------------------------------------------------------------
# DrcogSync.py
# ---------------------------------------------------------------------------
# Synchronize datasets from the DRCOG Regional Data Catalog to OpenColorado
#----------------------------------------------------------------------------

# Imports
import os
import urllib2
import ckanclient
import logging
from bs4 import BeautifulSoup

# Global variables
base_url = "http://gis.drcog.org"
data_catalog_prefix = "/datacatalog"
subjects_url_prefix = "/datacatalog/subjects/"
dataset_url_prefix = "/datacatalog/content/"
ckan_client = None

ckan_host = "http://test.opencolorado.org/api/2"
ckan_key = "5e6c9ea1-37fa-4b68-9412-5dbff28596bc"
ckan_group = "drcog"
ckan_title_prefix = "DRCOG: "
ckan_name_prefix = "drcog-"
ckan_license = "cc-by"

def main():
    
    # Get a list of subjects from the DRCOG catalog
    subjects = get_subjects()
    
    # Get all datasets by subject (datasets may be in more than one subject so there could be duplicates here)
    datasets = []
    for subject in subjects:
        datasets = datasets + get_dataset_urls_by_subject(subject)
        break # Just do the first subject for now
    
    # Remove duplicates
    datasets = list(set(datasets))
    
    # Begin syncing each dataset to CKAN
    for dataset in datasets:
        print dataset
        dataset_entity = get_dataset_entity(dataset)
        
        publish_to_ckan(dataset_entity)
        
        break #Just do the first dataset for now
        
def get_subjects():
    """Gets a list of subjects from the DRCOG data catalog

    Returns:
        List[string]
    """
    global base_url, subjects_url_prefix
    
    subjects = []
    subjects_url = base_url + "/datacatalog/content/welcome-regional-data-catalog?quicktabs_tabbed_menu_homepage=1"
    
    soup = get_soup_from_url(subjects_url)

    for link in soup.find_all('a'):
        href = link.get('href')
        if href.startswith(subjects_url_prefix):
            subject = href.replace(subjects_url_prefix,"")
            subjects.append(subject)
            
    return subjects

def get_dataset_urls_by_subject(subject, page_url=None):
    global base_url, subjects_url_prefix, dataset_url_prefix
    
    datasets = []
        
    datasets_url = base_url + subjects_url_prefix + subject
    
    if page_url != None:
        datasets_url = base_url + page_url
        
    print datasets_url
    
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

def get_dataset_entity(dataset):
    global base_url, dataset_url_prefix, ckan_title_prefix, ckan_name_prefix, ckan_license, data_catalog_prefix

    dataset_url = base_url + dataset_url_prefix + dataset
    
    dataset_entity = {}
    
    print dataset_url
    
    soup = get_soup_from_url(dataset_url)

    # Scrape the content from the dataset page
    dataset_entity['name'] = ckan_name_prefix + dataset
    dataset_entity['title'] = ckan_title_prefix + soup.find("h1",{ "id" : "page-title" }).getText()
    dataset_entity['license_id'] = ckan_license
    dataset_entity['url'] = base_url + data_catalog_prefix
    
    # Get descriptive information
    for field_item in soup.findAll("div", { "class" : "field-item" }):
        field_item_label = field_item.div.getText().strip().lower()
        field_item.div.extract()
        field_item_text = field_item.getText().strip()
        
        print field_item_label
        print field_item_text
        
        if (field_item_label.startswith("description:")):
            dataset_entity['notes'] = field_item_text
        elif (field_item_label.startswith("source:")):
            dataset_entity['author'] = field_item_text
        elif (field_item_label.startswith("contact name:")):
            dataset_entity['maintainer'] = field_item_text
        elif (field_item_label.startswith("contact email:")):
            dataset_entity['maintainer_email'] = field_item_text
    
    return dataset_entity

def publish_to_ckan(dataset_entity):
    """Updates the dataset in the CKAN repository or creates a new dataset

    Returns:
        None
    """
    global ckan_client, ckan_host, ckan_key
    
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
            print('Adding dataset to group: ' + ckan_group)      
            dataset_entity['groups'] = [group_entity['id']]
    except ckanclient.CkanApiNotFoundError:
        logger.warn('Group: ' + args.ckan_group_name + ' not found on OpenColorado')
        dataset_entity['groups'] = []     
     
    ckan_client.package_register_post(dataset_entity)

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
    
    ckan_client.package_entity_put(dataset_entity_remote)

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
        print("Dataset " + dataset_id + " found on OpenColorado")
        
    except ckanclient.CkanApiNotFoundError:
        print("Dataset " + dataset_id + " not found on OpenColorado")

    return dataset_entity

def get_soup_from_url(url):
    html = "".join(urllib2.urlopen(url).readlines())
    soup = BeautifulSoup(html)
    return soup

#Execute main function    
if __name__ == '__main__':
    main()