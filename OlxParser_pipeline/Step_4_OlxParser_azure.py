'''
Step IV
Step 4 is very simmilar to Step 1, but this time we are going to save pages from urls. They were not processed 
the first time.
This step was designed to save offer pages as html files into the mounted in Azure ML Pipeline folder.
For this purpose here was implemented the Tor dynamic IP rotation using the TorCrawler module. 
'''

# import all necessary libraries
import os
import json
import time
import argparse
import requests
import pandas as pd
from datetime import date
from TorCrawler import TorCrawler
from sqlalchemy import create_engine


# Define the argparser arguments for our Azure ML Pipeline:
# folder destination to store saved pages.
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pages-dir', dest='pages', required=True)
    return parser.parse_args()

args = parse_args()
path_pages = args.pages


# First, make sure the directory exists
os.makedirs(args.pages, exist_ok=True)


# Define configurations for requests
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36s'}
execution_date = date.today()

# Azure SQL connection string building
AZUREUID = 'vapak'                                    # Azure SQL database userid
AZUREPWD = 'OLXparsing2021'                           # Azure SQL database password
AZURESRV = 'webparsing.database.windows.net'   		  # Azure SQL database server name (fully qualified)
AZUREDB = 'webparsing'                                # Azure SQL database name (if it does not exit, pandas will create it)      
DRIVER = 'ODBC Driver 17 for SQL Server'              # ODBC Driver

# List of TABLES in he database
TABLE_BASE = 'OLXparsing_test'      							  # Azure SQL database table name   
TABLE_STAT = 'OLXparsing_status_test'      							  # Azure SQL database table name   


connectionstring = 'mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}'.format(
	uid=AZUREUID,
	password=AZUREPWD,
	server=AZURESRV,
	database=AZUREDB,
	driver=DRIVER.replace(' ', '+'))


engn = create_engine(connectionstring, fast_executemany=False)


# Create Tor crawler
crawler = TorCrawler(ctrl_pass='mypassword', n_requests=10)

# Necessary functions to implement Step 1:
def clean(text: str) -> str:
	"""
	This function clean text from \t and \n values
	Here we could add any additional transformation

	Args:
    	text (str):	Replace \t and \n values in given text
	
	Returns:
		text (str): Returns clean text without \t and \n signs

	"""	

	return text.replace('\t','').replace('\n','').strip()


# Have to change the function to save only unique pages
def save_offers_pages(offer_page_links: list) -> None:
	"""
	This function save each offer link as a separate html file from the list in the folder.

	Args:
		offer_page_links (list): List with all offer URLs from all pages.
	"""
	
	pages_dict = {}

	for num, offer_link in enumerate(offer_page_links):
		total_pages = len(offer_page_links)
		
		try:
			r = crawler.get(offer_link, headers=headers)
			error_403 = clean(r.find('h1').text)

			if error_403 == '403 ERROR':
				crawler.rotate()
				time.sleep(10)
				r = crawler.get(offer_link, headers=headers)
				with open("{}/page_{}.html".format(path_pages, num), "w") as f:
					f.write(str(r))
				print(f'Saved page №{num} of {total_pages} in {path_pages}')
				
			else:
				with open("{}/page_{}.html".format(path_pages, num), "w") as f:
					f.write(str(r))
				print(f'Saved page №{num} of {total_pages} in {path_pages}')

			# Append dict with url for each page
			pages_dict[f'page_{num}.html'] = offer_link
			


		except requests.exceptions.ConnectionError as e:
			print(f'RemoteDisconnected error: {e}')
			pass
		
		except Exception as e:
			print(f'Content was not found on page №{num} of {total_pages}')
			print(f'Exception occured: {e}')
			pass

	# Save our dict with page urls as json file into the same folder as html pages
	with open(f'{path_pages}/pages_dict.json', 'w') as fp:
		json.dump(pages_dict, fp)

def main():
	"""
	This function was desfigned to implement Step 4 cycle of OLX web page parsing.
	It takes main url page, search all offer links, save them as html pages.
	"""
	
	start_time = time.time()

	# Script starting
	print(f'OLX parsing started ..............')
	print(f'**********************************')
	print(f'STAGE 7: Parsing offer pages links')
	print(f'**********************************')


	
	query_pars =  "SELECT offer_id, offer_url "\
				"FROM dbo.OLXparsing_status_test "\
				"WHERE offer_status=1 " \
				"AND execution_date = '" + str(execution_date) + "'" \
				"AND offer_id NOT IN (SELECT DISTINCT offer_id FROM dbo.OLXparsing_test WHERE execution_date = '" + str(execution_date) +"')"


    
	dfsql = pd.read_sql(query_pars, engn)
	print(dfsql.head(15))

	dict_pages = dfsql.set_index('offer_id').to_dict()['offer_url']

	for k,v in dict_pages.items():
		print(f'{k}\t{v}')

	stemp_1 = time.time()
	print(f'\n Stage 1 execution time of the script:{(stemp_1 - start_time)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')


	# Saving all available offer pages
	print(f'**********************************')
	print(f'STAGE 8: Saving offer pages')
	print(f'**********************************')


	
	save_offers_pages(list(dict_pages.values()))
	
	stemp_2 = time.time()
	print(f'\n Stage 2 execution time of pages saving:{(stemp_2 - stemp_1)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')

	
# Execure while starting the script.py file
if __name__== '__main__':      
	main()