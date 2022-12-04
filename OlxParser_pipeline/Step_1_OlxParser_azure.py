'''
Step I
This step was designed to save offer pages as html files into the mounted in Azure ML Pipeline folder.
For this purpose here was implemented the Tor dynamic IP rotation using the TorCrawler module. 
'''

# import all necessary libraries
import os
import json
import time
import argparse
import requests
from typing import List
from datetime import date
from TorCrawler import TorCrawler


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
main_url = 'https://www.olx.ua/elektronika/telefony-i-aksesuary/mobilnye-telefony-smartfony/apple/q-iphone-7/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36s'}
execution_date = date.today()


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


def get_last_page_number(page_url: str) -> str:
	"""
	This function get the last page number in order to process
	parsing in range from first page to last

	Args:
		page_url (str): First page URL.
	
	Returns:
		last_num (str): The number of the last page
	"""

	r = crawler.get(page_url, headers=headers)
	cont = r.select('div.pager.rel.clr > span.item.fleft > a > span')

	try:
		last_num = cont[-1].string
	except:
		last_num = 25

	return last_num


def get_page_offer_links(page_url: str, page: str) -> List[str]:
	"""
	This is a complex funсtion to retrive all offer links.

	Args:
		page_url (str): Page url to scrap the offers URLs

	Returns:
		List[str]: List with all offer URLs on the page.
	"""
	
	print('get page links')
	
	r = crawler.get(page_url, headers=headers)

	print('crawler')

	table = r.find('table', {'id':'offers_table'})
	
	print('table')

	rows = table.find_all('tr' , {'class':'wrap'})
	result = []
	
	for row in rows:

		print('get urls')
		
		url = row.find('h3').find('a').get('href')
		result.append(url)
		
		print('links on page found')

	return result


def get_all_offer_links(main_url: str) -> List[str]:
	"""
	This finction is designed to parse all offer links from starting from the main page.

	Args:
		main_url (str): main page url where we start our process

	Returns:
		list: list with all links of all offers we are going to save to parse
	"""


	offer_page_links = [] # list to store our results to write in CSV
	page_num = int(get_last_page_number(main_url)) # But almost every time we achive 25 - the maximum displayed page number for OLX
	
	# for page in range(1, 2):
	for page in range(1, page_num + 1):

		print('Parsing offer links on page # ' + str(page) + ' of ' + str(page_num) + '\n')
		
		try:
			page_url = main_url + '?page=' + str(page)
			offer_page_links+=get_page_offer_links(page_url, page)
		except:
			print(f'There were no links on the {page} page')
			pass

	return offer_page_links


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


def main(main_url: str):
	"""
	This function was desfigned to implement Step 1 cycle of OLX web page parsing.
	It takes main url page, search all offer links, save them as html pages.

	Args:
		main_url (str): Takes an argument with main url page to process.
	"""
	
	start_time = time.time()

	# Script starting
	print(f'OLX parsing started ..............')
	print(f'**********************************')
	print(f'STAGE 1: Parsing offer pages links')
	print(f'**********************************')
	offer_pages_links = get_all_offer_links(main_url)

	# For control testing
	# offer_pages_links = offer_pages_links[:5]

	stemp_1 = time.time()
	print(f'\n Stage 1 execution time of the script:{(stemp_1 - start_time)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')
	time.sleep(60) # sleep for a while


	# Saving all available offer pages
	print(f'**********************************')
	print(f'STAGE 2: Saving offer pages')
	print(f'**********************************')

	# executor.map(save_offers_pages, offer_pages_links)
	
	save_offers_pages(offer_pages_links)
	
	stemp_2 = time.time()
	print(f'\n Stage 2 execution time of pages saving:{(stemp_2 - stemp_1)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')

	
# Execure while starting the script.py file
if __name__== '__main__':      
	main(main_url)