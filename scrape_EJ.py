#!/usr/bin/env python3

import json, re, traceback, shutil, send2trash
import pandas as pd
from datetime import datetime
from collections import deque
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.parse import quote  # to correct for instances when listing title is not in ascii encoding


def intojsonvar(jsonvar):
    """Cleans a json string of html markup
    """
    if jsonvar:
        try:
            jsonvar = re.sub(r"\r+|\n+|\t+|  |&nbsp;|amp;|</?.{,6}>", " ",jsonvar)
            jsonvar = re.sub(r"( ){2,}", r" ", jsonvar)
            jsonvar = re.sub(r"\u2019", r" '", jsonvar)
            jsonvar = re.sub(r"\u25cf", r" -", jsonvar)
            jsonvar = re.sub(r"\\",r"/", jsonvar)
            jsonvar = json.loads(jsonvar)
        except Exception as e:
            jsonvar = None
            print("problem converting into JSON. ERROR: ", str(e))
            # traceback.print_exc()
    else:
        pass
    return jsonvar


###############################################################
##                                                           ##
##                                                           ##
##                   SCRAPING ETHIOJOBS                      ##
##                                                           ##
##                                                           ##
###############################################################
listings_list = []

for page in range(1, 4):  # Up to page 3, can do up to page 4 when listings are numerous
    full_page = f"http://www.ethiojobs.net/latest-jobs-in-ethiopia/?searchId=1573067457.6526&action=search&page={ page }&listings_per_page=100&view=list"
    
    # Opening up connection, grabbing the page
    uClient = urlopen(full_page) 
    page_html = uClient.read()
    uClient.close() # Needed to close the client (connection)
    
    # Parsing the HTML with soup
    soup = BeautifulSoup(page_html, "lxml")
    
    # getting the link for each posting
    containers = soup.find_all( "tr" , {"class":"hidden-xs"} )
    Job_Links = [i.a["href"] for i in containers] 
    
    ########################################################
    #  Here's where the for-loop starts for all listings ###
    ########################################################
    status = 0
    
    for listing_url in Job_Links:
        
        try:
            
            # correction for cases when listing title is not in ascii encoding
            title_url = re.sub('http://www.ethiojobs.net/display-job/', "", listing_url)
            full_url = 'http://www.ethiojobs.net/display-job/' + quote(title_url)
            
            # Repeat the process by opening up the connection, and grabbing the page for the listing
            uClient = urlopen(full_url)  
            page_html = uClient.read()
            uClient.close() # Needed to close the client (connection)
            
            # Parsing the HTML with soup
            listing_soup = BeautifulSoup(page_html, "html")
            
            # ## Alternative places looked
            # containers = listing_soup.find("div",{"id":"listingResults"})
            # body = listing_soup.find("div",{"class":"listingInfoContent"})
            
            json_script = listing_soup.find("script", {"type":"application/ld+json"}).strings
            
            extracted_json_str = r''.join(json_script)                  
            
            # Convert to JSON
            json_listing = intojsonvar(extracted_json_str)
            
            ##########################################################
            # Add the header info (taken directly from the body, vs. the JSON-LD
            # metadata found in the HTML source code used above)
            header = listing_soup.find_all("div", {"class":"displayFieldBlock"})
            Header_listing ={}
            for i in range(0, len(header)): # <----- ENUMERATE THERE
                if header[i].find_all("div", {"class":"displaFieldHeader"})[0].text == 'Category:':
                    Header_listing["category_tags"] = header[i].find_all("div", {"class":"displayField"})[0].text
                elif header[i].find_all("div", {"class":"displaFieldHeader"})[0].text == 'Location:':
                    Header_listing["location"] = header[i].find_all("div", {"class":"displayField"})[0].text
                elif header[i].find_all("div", {"class":"displaFieldHeader"})[0].text == 'Career Level:':
                    Header_listing["career_level"] = header[i].find_all("div", {"class":"displayField"})[0].text
                elif header[i].find_all("div", {"class":"displaFieldHeader"})[0].text == 'Employment Type:':
                    Header_listing["employment_type"] = header[i].find_all("div", {"class":"displayField"})[0].text
                elif header[i].find_all("div", {"class":"displaFieldHeader"})[0].text == 'Salary:':
                    Header_listing["salary"] = header[i].find_all("div", {"class":"displayField"})[0].text
            
            # convert to JSON string python object
            Header_listing = json.dumps(Header_listing)
                
            # clean up string
            Header_listing = re.sub(pattern = r"\\.|00a0", repl = "", string = Header_listing)
            # save to JSON and add to json_listing
            Header_listing = json.loads(Header_listing)    
            # Fill in 'None' for missing entries
            Header_keys = ['category_tags', 'location', 'career_level', 'salary', 'employment_type']    
            for i in Header_keys:
                if i not in Header_listing:
                    Header_listing[i]=None
                                
            json_listing["Header_listing"] = Header_listing
            json_listing["url"] = full_url
            
            # Scrape View count #
            sub_title_header = listing_soup.find("span", {"class":"jobs_by"}).strings
            sub_title_header = r''.join(sub_title_header) # get string format
            
            views_search = re.search(r'\| ([0-9]{1,}) V', sub_title_header, re.IGNORECASE)
            
            if views_search:
                views = int(views_search.group(1))
            else:
                views = None
            
            json_listing["views"] = views
            
            # Append final JSON to list
            listings_list.append(json_listing)
            
            status += 1
            print("Added listing ", status, ", for page", page)
            
        except Exception as e:
            status += 1
            print(f"ERROR for listing {status} on page {page}. ERROR MESSAGE: {str(e)}")
            pass

out_filename = r'DATA_EthioJobsListings_{0}.json'.format(f"{datetime.now():%m-%d-%Y-%H%M}")
with open(out_filename, 'w') as outfile:
    json.dump(listings_list, outfile)


###############################################################
##                                                           ##
##                                                           ##
##                   CLEANING ETHIOJOBS                      ##
##                                                           ##
##                                                           ##
###############################################################


json_list = json.load(open(out_filename))

full_json_to_pd = []

for i in json_list:
    
    new_dict = {
        "title": i['title'],
        "job_description": i["description"],
        "org_name": i["identifier"]["name"],
        "datePosted": i['datePosted'],
        "validThrough": i['validThrough'],
        "addressRegion": i['jobLocation']["address"]["addressRegion"],
        "addressCountry": i['jobLocation']["address"]["addressCountry"],
        "employmentType": i["employmentType"],
        "url": i["url"],
        "views": i["views"]
    }
    for j in i["Header_listing"]:
        new_dict[j] = i["Header_listing"][j]
    
    full_json_to_pd.append(new_dict)


full_json_data = json.loads(json.dumps(full_json_to_pd))


cleaned_file_name = f"Cleaned_Data_EthioJobsListings_{datetime.now():%m-%d-%Y-%H%M}.json"
with open(cleaned_file_name, 'w') as outfile:
    json.dump(full_json_data, outfile)

dataset = pd.read_json(cleaned_file_name).drop_duplicates(subset=['datePosted','url'])



###############################################################
##                                                           ##
##                                                           ##
##  Mergring "employment_type" and "employmentType" columns  ##
##                                                           ##
##                                                           ##
###############################################################



# First, clean 'Full Time' entries to 'Full time' : ##

# Create BOOLEAN INDEX for Nan values in 'employment_type'
NaN_values = dataset["employment_type"].isna()

# Transfer corresponding elements from 'employmentType' over into 'employment_type'to fill in the NaN's
dataset["employment_type"][NaN_values] = dataset["employmentType"][NaN_values].copy()
dataset.drop(columns=['employmentType'], inplace= True)
#   ## (OR, DO IT ONE LINE)
# dataset["employment_type"][NaN_values] = dataset.loc[dataset["employment_type"].isna(), "employmentType"]

#### Replace 'time' with 'Time' 
dataset["employment_type"] = dataset["employment_type"].apply(lambda x:x.replace("Time","time"))

#### Create first/secondary tag category column i.e. 'category_primary' & 'category_secondary'
split_categories = dataset['category_tags'].str.split(',', expand=True)
cat_primary = split_categories[0]
cat_secondary = split_categories[1]
dataset["category_tags"] = cat_primary
dataset.rename(columns = {'category_tags' : 'category_primary'}, inplace=True)
dataset.insert(9, 'category_secondary', cat_secondary)


######################################################################
# Change date data to Datetime, and 'id' to float, and 'views' to int (Needed because the data
# types keep changing when reading)
def change_types(dataframe):
    dataframe['datePosted'] = pd.to_datetime(dataframe['datePosted'])
    dataframe['validThrough'] = pd.to_datetime(dataframe['validThrough'])
    # dataframe['id'] = dataframe['id'].astype(float)
    dataframe['views'] = dataframe['views'].astype(int)
    dataframe = dataframe.sort_values(by=['datePosted'], ascending=False)

change_types(dataset)


# Save final product
dataset.to_csv(f"EthioJobs_Listings_{datetime.now():%m-%d-%Y-%H%M}.csv", index=False)

# delete previous jsons
send2trash.send2trash(out_filename)
send2trash.send2trash(cleaned_file_name)

# END #
print("Job Completed")
