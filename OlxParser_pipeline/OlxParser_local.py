'''
This script is used to parse OLX results of iphone 7 searching. 
It will help you to save images from links and parsed pages in html.
Text data from the page will be save in separate CSV file in the same folder.
'''

# import all necessary libraries
import os
import bs4
import time
import json
import requests
import sqlalchemy
import pandas as pd
import urllib.request
from bs4 import BeautifulSoup
from typing import List
from TorCrawler import TorCrawler
from sqlalchemy import create_engine
from datetime import date, datetime, timedelta



# Start Tor session and stop command to execute in terimal
# service tor start
# service tor stop


# Define variables and dependencies
main_url = 'https://www.olx.ua/elektronika/telefony-i-aksesuary/mobilnye-telefony-smartfony/apple/q-iphone-7/'
headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36s'}
execution_date = date.today()
img_folder = 'images'
page_folder = 'pages'

# NBU API for exchange rates
nbu_api = 'https://bank.gov.ua/NBUStatService/v1/statdirectory/exchange?json'
nbu_json = json.loads(requests.get(nbu_api, headers=headers, verify=False).text)

# Azure SQL connection string building
AZUREUID = 'vapak'                                    # Azure SQL database userid
AZUREPWD = 'OLXparsing2021'                           # Azure SQL database password
AZURESRV = 'webparsing.database.windows.net'   		  # Azure SQL database server name (fully qualified)
AZUREDB = 'WebParsing'                                # Azure SQL database name (if it does not exit, pandas will create it)      
TABLE = 'OLXparsing'      							  # Azure SQL database table name                             
DRIVER = 'ODBC Driver 17 for SQL Server'              # ODBC Driver

connectionstring = 'mssql+pyodbc://{uid}:{password}@{server}:1433/{database}?driver={driver}'.format(
	uid=AZUREUID,
	password=AZUREPWD,
	server=AZURESRV,
	database=AZUREDB,
	driver=DRIVER.replace(' ', '+'))


# Create Tor crawler
crawler = TorCrawler(ctrl_pass='mypassword', n_requests=10)


# We create a folder to store images
if not os.path.exists(img_folder):
    os.makedirs(img_folder)


# Folder to store offer pages
if not os.path.exists(page_folder):
    os.makedirs(page_folder)	



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



def get_last_page_number(page_url: str) -> str:
	"""
	This function get the last page number in order to process
	parsing in range from first page to last

	Parameters
	----------
	page_url : str
		First page URL.
	"""

	r = crawler.get(page_url, headers=headers)
	cont = r.select('div.pager.rel.clr > span.item.fleft > a > span')
	last_num = cont[-1].string
	return last_num


def get_page_offer_links(page_url: str, page: int) -> List[str]:
	'''
	This is a complex funсtion to retrive all offer links.

	Parameters
	----------
	page_url : 
		Every page url to scrap the data
	page :
		page number for naming
	'''

	r = crawler.get(page_url, headers=headers)
	# soup = BeautifulSoup(r.content,'lxml')
	table = r.find('table', {'id':'offers_table'})
	rows = table.find_all('tr' , {'class':'wrap'})
	result = []
	
	for row in rows:
		url = row.find('h3').find('a').get('href')
		result.append(url)
		
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

	for page in range(1, page_num + 1):
		print('Parsing offer links on page # ' + str(page) + ' of ' + str(page_num) + '\n')
				
		page_url = main_url + '?page=' + str(page)

		offer_page_links+=get_page_offer_links(page_url, page)

	return offer_page_links


# Have to change the function to save only unique pages
def save_offers_pages(offer_page_links: list) -> None:
	'''
	This function allows to save parsing pages in html in separate folder. 
	
	Parameters
	----------
	soup : 
		result of a BeautifulSoup work
	page_folder :
		define a folder to save results
	page_num :
		page number for naming
	'''
	
	for num, offer_link in enumerate(offer_page_links):
		total_pages = len(offer_page_links)
		r = crawler.get(offer_link, headers=headers)
		try:
			error_403 = clean(r.find('h1').text)

			if error_403 == '403 ERROR':
				crawler.rotate()
				time.sleep(10)
				r = crawler.get(offer_link, headers=headers)
				with open("{}/page_{}.html".format(page_folder, num), "w") as file:
					file.write(str(r))
				print(f'Saved page №{num} of {total_pages} in {page_folder}')
				
			else:
				with open("{}/page_{}.html".format(page_folder, num), "w") as file:
					file.write(str(r))
				print(f'Saved page №{num} of {total_pages} in {page_folder}')
		except:
			print(f'Content was not found on page №{num} of {total_pages}')



# Open saved offer pages
def open_html(file: str) -> str:
	'''
	This function clean opens saved offer pages

	Parameters
    ----------
    file : str
		File or path to file to open
	'''
	with open(file) as fp:
		soup = BeautifulSoup(fp, 'lxml')
	return soup


def save_images(img_folder: str, offer_id: str, soup: bs4.BeautifulSoup) -> List[str]:
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

	no_image_link = 'https://upload.wikimedia.org/wikipedia/commons/1/14/No_Image_Available.jpg'

	# Create ID folder to store images
	if not os.path.exists(img_folder + '/' + offer_id):
		os.makedirs(img_folder + '/' + offer_id)

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
				urllib.request.urlretrieve(img_link, '{}/{}/image_{}.jpg'.format(img_folder, offer_id, i))
			except urllib.error.HTTPError: # To hadle errors with urllib
				pass
	
	# If we could not find wrapper, we assing this image
	except:
		opener=urllib.request.build_opener()
		opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.159 Safari/537.36s')]
		urllib.request.install_opener(opener)
		urllib.request.urlretrieve(no_image_link, '{}/{}/image_0.jpg'.format(img_folder, offer_id))


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

	# dict to map months
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
		'декабря':12
	}

	# Some dates are uncommon
	if 'сегодня' in item:
		item = datetime.now().strftime('%Y-%m-%d')
	elif 'вчера' in item:
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
		'декабрь':12
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

	
	if 'обмен' in price:
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
	final_df = pd.DataFrame(columns=['execution_date', 'offer_id', 'publication_date', 'name', 'price',
								'offer_info', 'tag_seller', 'tag_brand', 'tag_os', 'tag_screen', 
								'tag_condition', 'seller_name', 'seller_hist'])

	offer_pages = os.listdir(folder_path)

	# offer_pages = ['page_33.html'] # here to work only with on page for testing

	offer_pages.sort()

	for offer_page in offer_pages:
		print(f'Parsing saved page: {offer_page}')

		try:
			soup = open_html(folder_path + '/' + offer_page)

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
			urls = save_images(img_folder, offer_id, soup)

			# Seller details
			seller_name = clean(soup.find('h2', {'class':'css-u8mbra-Text'}).text)
			seller_hist = clean(soup.find('div', {'class':'css-1bafgv4-Text'}).text)
			seller_hist = parse_seller_hist_date(seller_hist)


			# Dataframe to store results from each page
			final_df = final_df.append({'execution_date': execution_date, 
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

		except:
			print(f'Error on {offer_page}') # Print unprocessed pages with Errors in structure
			pass

	return final_df



def main(main_url: str):
	"""
	This function was desfigned to implement all cycle of OLX web page parsing.
	It takes main url page, search all offer links, save them as html pages.
	Process them and stores results in pandas DataFrame. In the end we store this data
	in Azure SQL and images from offers in separate folders with IDs.

	Args:
		main_url (str): Takes an argument with main url page to process
	"""
	
	start_time = time.time()

	# Script starting
	print(f'OLX parsing started ..............')
	print(f'**********************************')
	print(f'STAGE 1: Parsing offer pages links')
	print(f'**********************************')
	offer_pages_links = get_all_offer_links(main_url)
	stemp_1 = time.time()
	print(f'\n Stage 1 execution time of the script:{(stemp_1 - start_time)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')
	time.sleep(60) # sleep for a while


	# Saving all available offer pages
	print(f'**********************************')
	print(f'STAGE 2: Saving offer pages')
	print(f'**********************************')
	save_offers_pages(offer_pages_links)
	stemp_2 = time.time()
	print(f'\n Stage 2 execution time of pages saving:{(stemp_2 - stemp_1)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')


	# Parsing all offer pages
	print(f'**********************************')
	print(f'STAGE 3: Parsing saved offer pages')
	print(f'**********************************')
	dataframe = parse_offer_pages(page_folder)
	dataframe.drop_duplicates(subset=['offer_id', 'publication_date'], keep='first', inplace=True)
	stemp_3 = time.time()
	print(f'\n Stage 3 execution time of pages saving:{(stemp_3 - stemp_2)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')


	# Saving all pages to the Azure SQL database
	print(f'**********************************')
	print(f'STAGE 4: Saving results to Azure')
	print(f'**********************************')

	dataframe.to_csv('database.csv', sep='\t', header=True, index=False) # create backup with csv
	# dataframe = pd.read_csv('database.csv', sep='\t', index_col='execution_date')

	engn = create_engine(connectionstring, fast_executemany=False)

	dataframe.to_sql(TABLE,
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


    # 5. Read data from SQL into dataframe
	query = 'SELECT * FROM {table}'.format(table=TABLE)
	dfsql = pd.read_sql(query, engn)
	print(dfsql.head())


	stemp_4 = time.time()
	print(f'\n Stage 4 execution time of pages saving:{(stemp_4 - stemp_3)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')

	print(f'All stages exucution times')
	print(f'\n Stage 1 (Parsing offer pages links) execution time of the script:{(stemp_1 - start_time)}')
	print(f'\n Stage 2 (Saving offer pages) execution time of pages saving:{(stemp_2 - stemp_1)}')
	print(f'\n Stage 3 (Parsing saved offer pages) execution time of pages saving:{(stemp_3 - stemp_2)}')
	print(f'\n Stage 4 (Saving results to Azure) execution time of pages saving:{(stemp_4 - stemp_3)}')

	
# Execure while starting the script.py file
if __name__== '__main__':      
	main(main_url)