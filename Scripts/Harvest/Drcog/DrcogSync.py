# ---------------------------------------------------------------------------
# DrcogSync.py
# ---------------------------------------------------------------------------
# Synchronize datasets from the DRCOG Regional Data Catalog to OpenColorado
#----------------------------------------------------------------------------

# Imports
import os
import urllib2
from bs4 import BeautifulSoup

# Global variables
base_url = "http://gis.drcog.org"
subjects_url_prefix = "/datacatalog/subjects/"
dataset_url_prefix = "/datacatalog/content/"

def main():
    
    # Get a list of subjects from the DRCOG catalog
    subjects = get_subjects()
    
    # Get all datasets by subject (datasets may be in more than one subject so there could be duplicates here)
    datasets = []
    for subject in subjects:
        datasets = datasets + get_dataset_urls_by_subject(subject)
    
    # Remove duplicates
    datasets = list(set(datasets))
    
    # Begin syncing each dataset to CKAN
    for dataset in datasets:
        print dataset
        
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

def get_soup_from_url(url):
    html = "".join(urllib2.urlopen(url).readlines())
    soup = BeautifulSoup(html)
    return soup

#Execute main function    
if __name__ == '__main__':
    main()