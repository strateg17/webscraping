'''
Step II
This step was designed to parse saved offer pages from html files into the mounted in Azure ML Pipeline folder.
Images from offers are saved into the Azure Bloob Storage
'''

# import all necessary libraries
import os
import bs4
import time
import json
import argparse
import requests
import sqlalchemy
import pandas as pd
import urllib.request
from typing import List
from pathlib import Path
from bs4 import BeautifulSoup
from TorCrawler import TorCrawler
from sqlalchemy import create_engine
from datetime import date, datetime, timedelta
from azure.storage.blob import BlockBlobService



# Start Tor session and stop command to execute in terimal
# service tor start
# service tor stop


# Define the argparser arguments for our Azure ML Pipeline
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--pages-dir', dest='pages', required=True)
    parser.add_argument('--images-dir', dest='images', required=True)

    return parser.parse_args()


args = parse_args()
path_pages = Path(args.pages)
path_images = Path(args.images)


# First, make sure the directory exists
if not os.path.exists(path_images):
        os.makedirs(path_images)

print('-----------------------------------------------')
print(f'This is our temp path for pages: {path_pages}')
print(f'This is our temp path for pages: {path_images}')
print('-----------------------------------------------')


# Credentials for Blob Container
container_name = os.getenv("BLOB_CONTAINER")
account_name = os.getenv("BLOB_ACCOUNTNAME")
account_key = os.getenv("BLOB_ACCOUNT_KEY")


block_blob_service = BlockBlobService(
    account_name=account_name,
    account_key=account_key
)


# Define variables and dependencies
main_url = 'https://www.olx.ua/elektronika/telefony-i-aksesuary/mobilnye-telefony-smartfony/apple/q-iphone-7/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36s'}
execution_date = date.today()
img_folder = 'images'
page_folder = 'pages'


# NBU API for exchange rates
nbu_api = 'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json'
nbu_json = json.loads(requests.get(nbu_api, headers=headers, verify=False).text)


# dictonary with pages and their urls
with open(f'{path_pages}/pages_dict.json', 'r') as fp:
	pages_dict = json.load(fp)


# Azure SQL connection string building
AZUREUID = 'vapak'                                    # Azure SQL database userid
AZUREPWD = 'OLXparsing2021'                           # Azure SQL database password
AZURESRV = 'webparsing.database.windows.net'   		  # Azure SQL database server name (fully qualified)
AZUREDB = 'webparsing'                                # Azure SQL database name (if it does not exit, pandas will create it)      
DRIVER = 'ODBC Driver 17 for SQL Server'              # ODBC Driver

# List of TABLES in he database
TABLE_BASE = 'OLXparsing_test'      							  # Azure SQL database table name   
TABLE_STAT = 'OLXparsing_status_test'  							  # Azure SQL database table name   


connectionstring = 'mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}'.format(
	uid=AZUREUID,
	password=AZUREPWD,
	server=AZURESRV,
	database=AZUREDB,
	driver=DRIVER.replace(' ', '+'))


# Create Tor crawler
# crawler = TorCrawler(ctrl_pass='mypassword', n_requests=10)


def clean(text: str) -> str:
	"""
	This function clean text from \t and \n values
	Here we could add any additional transformation

	Parameters
    ----------
    text : str
		Replace \t and \n values in given text
	"""	

	return text.replace('\t','').replace('\n','').strip()


# Open saved offer pages
def open_html(file_name: str) -> str:
	"""
	This function clean opens saved offer pages

	Parameters
    ----------
    file : str
		File or path to file to open
	"""
	with open(file_name) as fp:
		soup = BeautifulSoup(fp, 'lxml')
	return soup


def save_images(path_images: str, offer_id: str, soup: bs4.BeautifulSoup) -> List[str]:
	"""
	This function allows to save get image url and download it. 
	In case there is no image we save specific image.

	Parameters
	----------
	img_folder :
		define a folder to save results
	offer_id :
		offer_id is necessary to save images in unique ID folder
	soup :
		html page open with BeautifulSoup
	"""

	os.makedirs(os.path.join(path_images, offer_id), exist_ok=True) # create folder

	no_image_link = 'https://upload.wikimedia.org/wikipedia/commons/1/14/No_Image_Available.jpg'

	# list to save all image links
	img_links = []

	# try to find wrapper with images
	try:
		
		wrapper = soup.find('div', {'class':'swiper-wrapper'}).find_all('div',{'class':'swiper-slide'})
		for ele in wrapper:
			if ele.find('img', {'class':'css-1bmvjcs'}).get('src'): # For first image we habe 'src'
				url = ele.find('img', {'class':'css-1bmvjcs'}).get('src')
				img_links.append(url)

			elif ele.find('img', {'class':'css-1bmvjcs'}).get('data-src'): # For all rest images 'data-src'
				url = ele.find('img', {'class':'css-1bmvjcs'}).get('data-src')
				img_links.append(url)

			else: # If we have wrapper, but no images at all assign this image
				img_links.append(no_image_link)

		# Download images from the img_links list
		for i, img_link in enumerate(img_links):
			try: 
				opener=urllib.request.build_opener()
				opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36s')]
				urllib.request.install_opener(opener)

				print('{}/{}/image_{}.jpg'.format(str(path_images), offer_id, i))
				
				urllib.request.urlretrieve(img_link, os.path.join(path_images,offer_id, f'image_{i}.jpg'))
			except urllib.error.HTTPError: # To hadle errors with urllib
				print('pass error')
				pass
	
	# If we could not find wrapper, we assing this image
	except:
		
		opener=urllib.request.build_opener()
		opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36s')]
		urllib.request.install_opener(opener)
		urllib.request.urlretrieve(no_image_link, os.path.join(path_images,offer_id, f'image_{i}.jpg'))


	# After downloading all possible images we simultaneously upload them into the Azure Blob Storage
	# all folders in the pages directory
	file_names = os.listdir(os.path.join(path_images, offer_id))
	print('Loading to blob')
	for file_name in file_names:
		blob_name = f"images/{offer_id}/{file_name}"
		file_path = f"{path_images}/{offer_id}/{file_name}"

		print(blob_name)
		print(file_path)

		block_blob_service.create_blob_from_path(container_name, blob_name, file_path)

	return img_links


def parse_offer_date(item: str) -> str:
	"""
	This function allows to convert offer date to datetime format.

	Parameters
	----------
	item :
		initial offer date from the page
	"""

	# convert all to lower case
	item = [el.lower() for el in item.split()]

	# dict to map months for Russian and UKrainian versions
	month_dict = {
		'января':1,
		'февраля':2,
		'марта':3,
		'апреля':4,
		'мая':5,
		'июня':6,
		'июля':7,
		'августа':8,
		'сентября':9,
		'октября':10,
		'ноября':11,
		'декабря':12,

		'січня':1,
		'лютого':2,
		'березня':3,
		'квітня':4,
		'травня':5,
		'червня':6,
		'липня':7,
		'серпня':8,
		'вересня':9,
		'жовтня':10,
		'листопада':11,
		'груденя':12
	}

	# Some dates are uncommon
	if 'сегодня' in item or 'сьогодні' in item:
		item = datetime.now().strftime('%Y-%m-%d')
	elif 'вчера' in item or 'вчора' in item:
		item = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
	else:
		day = int(item[1])
		month_hru = item[2]
		year = int(item[3])

		month = int(month_dict.get(month_hru))
		item = datetime(year=year, month=month, day=day).strftime('%Y-%m-%d')
	return item


def parse_seller_hist_date(item: str) -> str:
	"""
	This function allows to convert seller on OLX date to datetime format.

	Parameters
	----------
	item :
		initial date from the page
	"""

	# convert all to lower case
	item = [el.lower() for el in item.split()]

	month_dict = {
		'январь':1,
		'февраль':2,
		'март':3,
		'апрель':4,
		'май':5,
		'июнь':6,
		'июль':7,
		'август':8,
		'сентябрь':9,
		'октябрь':10,
		'ноябрь':11,
		'декабрь':12,
		
		'січень':1,
		'лютий':2,
		'березень':3,
		'квітень':4,
		'травень':5,
		'червень':6,
		'липень':7,
		'серпень':8,
		'вересень':9,
		'жовтень':10,
		'листопад':11,
		'грудень':12
	}
	
	day=1
	month_hru = item[3]
	year = int(item[4])

	month = int(month_dict.get(month_hru))
	item = datetime(year=year, month=month,day=day).strftime('%Y-%m-%d')
	return item


def parse_offer_price(price: str) -> float:
	"""
	This function was designed to convert string format price into the
	more appropriate float format and convert USD / EUR to UAH.

	Args:
		price (str): price in string format from the page parsing

	Returns:
		float: the same price but in 
	"""
	price = [el.lower() for el in price.split()]
	
	if '$' in price:
		exchange_rate = nbu_json[26]['rate'] # USD
	elif '€' in price:
		exchange_rate = nbu_json[32]['rate'] # EUR
	else:
		exchange_rate = 1

	
	if 'обмен' in price or 'обмін' in price:
		price = 0
	else:
		price = round(float(''.join(price[:-1])) * exchange_rate, 2)
	return price



def parse_offer_pages(folder_path: str) -> pd.DataFrame:
	"""
	This function reads each page in the folder and parse data from it and return
	result as a dataframe.

	Args:
		folder_path (str): Path to the folder with saved offer pages

	Returns:
		dataframe: Dataframe with info from every processed page
	"""
	base_df = pd.DataFrame(columns=['execution_date', 'offer_id', 'publication_date', 'name', 'price',
								'offer_info', 'tag_seller', 'tag_brand', 'tag_os', 'tag_screen', 
								'tag_condition', 'seller_name', 'seller_hist'])

	status_df = pd.DataFrame(columns=['execution_date', 'offer_id', 'offer_url', 'offer_status'])


	print('All in cwd: ', os.listdir())
	all_files = os.listdir(folder_path)

	offer_pages = [ fname for fname in all_files if fname.endswith('.html')] # filter only html files
	
	offer_pages.sort()
	
	for offer_page in offer_pages:

		print(f'Parsing saved page: {folder_path}/{offer_page}')
		
		try:
			print(os.path.join(folder_path, offer_page))
			soup = open_html(os.path.join(folder_path, offer_page))
			
			# offer details
			offer_id = clean(soup.find('span', {'class':'css-9xy3gn-Text'}).text).split()[1]

			# publication date
			publication_date = clean(soup.find('div', {'class':'css-sg1fy9'}).text)
			publication_date = parse_offer_date(publication_date)

			# offer name
			name = clean(soup.find('h1').text)
			
			# offer price
			price = clean(soup.find('h3', {'class':'css-okktvh-Text'}).text)
			price = parse_offer_price(price)

			offer_info = clean(soup.find('div', {'class':'css-g5mtbi-Text'}).text)
			tags = soup.find_all('p', {'class':'css-xl6fe0-Text'})

			tags = [clean(tag.text) for tag in tags]
			tag_seller = tags[0]
			tag_brand = tags[1].split()[-1]
			tag_os = tags[2].split()[-1]
			tag_screen = tags[3].split()[-1]
			tag_condition = tags[4].split()[-1]

			# offer image saving
			urls = save_images(os.path.join(path_images), offer_id, soup)

			# Seller details
			seller_name = clean(soup.find('h2', {'class':'css-u8mbra-Text'}).text)
			seller_hist = clean(soup.find('div', {'class':'css-1bafgv4-Text'}).text)
			seller_hist = parse_seller_hist_date(seller_hist)

			# Retrive pages url
			page_url = pages_dict[offer_page]

			# Assign default offer status
			status = 1

			# Dataframe to store results from each page
			base_df = base_df.append({'execution_date': execution_date, 
										'offer_id':offer_id, 
										'publication_date':publication_date, 
										'name':name, 
										'price':price,
										'offer_info':offer_info, 
										'tag_seller':tag_seller, 
										'tag_brand':tag_brand, 
										'tag_os':tag_os, 
										'tag_screen':tag_screen, 
										'tag_condition':tag_condition, 
										'seller_name':seller_name, 
										'seller_hist':seller_hist}, 
										ignore_index=True)  

			status_df = status_df.append({'execution_date' : execution_date, 
										  'offer_id' : offer_id, 
										  'offer_url': page_url,
										  'offer_status': status},
										  ignore_index=True)

		except Exception as e:
			print(f'Error on {offer_page}') # Print unprocessed pages with Errors in structure
			print(e)
			pass

	return base_df, status_df



def main():
	"""
	Process pages them and stores results in pandas DataFrame. In the end we store this data
	in Azure SQL and images from offers in separate folders with IDs.
	"""
	
	start_time = time.time()
	stemp_2 = time.time()
	# Parsing all offer pages
	print(f'**********************************')
	print(f'STAGE 3: Parsing saved offer pages')
	print(f'**********************************')
	df_base, df_status = parse_offer_pages(os.path.join(path_pages))


	df_base.drop_duplicates(subset=['offer_id', 'publication_date'], keep='first', inplace=True)
	df_status.drop_duplicates(subset=['execution_date', 'offer_id'], keep='first', inplace=True)


	stemp_3 = time.time()
	print(f'\n Stage 3 execution time of pages saving:{(stemp_3 - stemp_2)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')


	# # Saving all pages to the Azure SQL database
	print(f'**********************************')
	print(f'STAGE 4: Saving results to Azure')
	print(f'**********************************')

	# This block for reasons if you want to save and load data from csv
	# # dataframe.to_csv('database.csv', sep='\t', header=True, index=False) # create backup with csv
	# # dataframe = pd.read_csv('database.csv', sep='\t', index_col='execution_date')

	engn = create_engine(connectionstring, fast_executemany=False)

	#  Load data from pages into the Azure SQL Database
	df_base.to_sql(TABLE_BASE,
			engn, 
			if_exists='append',
			method='multi',
			chunksize=100, # magic number
			dtype={'execution_date': sqlalchemy.Date(), 
					'offer_id': sqlalchemy.types.Integer(),
					'publication_date': sqlalchemy.Date(),
					'name': sqlalchemy.types.UnicodeText(),
					'price': sqlalchemy.types.Float(precision=2),
					'offer_info': sqlalchemy.types.UnicodeText(),
					'tag_seller': sqlalchemy.types.UnicodeText(),
					'tag_brand': sqlalchemy.types.UnicodeText(),
					'tag_os': sqlalchemy.types.UnicodeText(),
					'tag_screen': sqlalchemy.types.UnicodeText(),
					'tag_condition': sqlalchemy.types.UnicodeText(),
					'seller_name': sqlalchemy.types.UnicodeText(),
					'seller_hist': sqlalchemy.Date()},
			index=False)

	#  Load offer status and urls
	df_status.to_sql(TABLE_STAT,
				engn, 
				if_exists='append',
				method='multi',
				chunksize=100, # magic number
				dtype={'execution_date': sqlalchemy.Date(), 
					'offer_id': sqlalchemy.types.UnicodeText(),
					'offer_url': sqlalchemy.types.UnicodeText(),
					'offer_status': sqlalchemy.types.Integer()},
			index=False)


    # 5. Read data from SQL into dataframe
	query = 'SELECT * FROM {table}'.format(table=TABLE_BASE)
	dfsql = pd.read_sql(query, engn)
	print(dfsql.head())


	stemp_4 = time.time()
	print(f'\n Stage 4 execution time of pages saving:{(stemp_4 - stemp_3)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')


# Execure while starting the script.py file
if __name__== '__main__':      
	main()