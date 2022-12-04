"""
Step III
This step was designed to verify all not processed today offers and verify their status.
"""
# import all necessary libraries
import time
import requests
import sqlalchemy
import pandas as pd
from TorCrawler import TorCrawler
from sqlalchemy import create_engine
from datetime import date

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


def test_offers_pages(dict_pages: dict) -> pd.DataFrame:
	"""
	This function tests whether ID still active and return two thing: 
	dictionary with pages we should process additionally and dataframe with deactivated pages to store in the Azure SQL

	Args:
		dict_pages (dict): Dictionary with unprocessed pages on today execution date.
	
	Returns:
		status_df (pd.DataFrame): 
	"""

		
	status_df = pd.DataFrame(columns=['execution_date', 'offer_id', 'offer_url', 'offer_status']) # DataFrame to 

	for offer_id, offer_link in dict_pages.items():
		
		try:
			# Check for 403 connection block from the OLX
			error_403 = '403 ERROR'

			r = crawler.get(offer_link, headers=headers)

			
			# We catch 403 -> rotate IP
			if error_403 == clean(r.find('h1').text):
				crawler.rotate()
				time.sleep(10)

				# search for deactivation text

				r = crawler.get(offer_link, headers=headers)
				
				try:
					offer_id = clean(r.find('span', {'class':'css-9xy3gn-Text'}).text).split()[1]
					status=1
				except :
					status=0
			
			# We haven't catch 403 error
			else:

				# search for deactivation text

				r = crawler.get(offer_link, headers=headers)

				try:

					offer_id = clean(r.find('span', {'class':'css-9xy3gn-Text'}).text).split()[1]
					status=1

				except :
					status=0

			print(f'{execution_date}\t{offer_id}\t{status}\t{offer_link}')

			status_df = status_df.append({'execution_date' : execution_date, 
										'offer_id' : offer_id, 
										'offer_url' : offer_link, 
										'offer_status': status}, ignore_index=True)
	
		# For full connection error
		except requests.exceptions.ConnectionError as e:
			print(f'RemoteDisconnected error for offer: {offer_id}')
			pass


		# For None in the deactivated offers
		except:
			r = crawler.get(offer_link, headers=headers)
				
			try:
				offer_id = clean(r.find('span', {'class':'css-9xy3gn-Text'}).text).split()[1]
				status=1
			except :
				status=0

			print(f'{execution_date}\t{offer_id}\t{status}\t{offer_link}')

			# Append dataframe
			status_df = status_df.append({'execution_date' : execution_date, 
													'offer_id' : offer_id, 
													'offer_url' : offer_link, 
													'offer_status': status}, ignore_index=True)
	print('\n')
	return status_df



def main():
	"""
	This function was designed to verify how many pages was scrapped during the execution date.
	"""

	start_time = time.time()
	stemp_2 = time.time()
	# Parsing all offer pages
	print(f'**********************************')
	print(f'STAGE 5: SELECT IDs not processed today')
	print(f'**********************************')

	# may be add not all offer ID and only previous active offers
	# SELECT DISCTINCT offer_id AND page_url
	query_test =  "SELECT offer_id, offer_url "\
				"FROM dbo.OLXparsing_status_test "\
				"AND offer_status = 1"\
				"WHERE offer_id NOT IN (SELECT DISTINCT offer_id FROM dbo.OLXparsing_status_test WHERE execution_date = '" + str(execution_date) +"')"

	# select necessary offer IDs
	dfsql = pd.read_sql(query_test, engn)
	dict_pages = dfsql.set_index('offer_id').to_dict()['offer_url']
	print(dict_pages.keys(), sep=',')

	start_time = time.time()
	stemp_2 = time.time()
	# Parsing all offer pages
	print(f'**********************************')
	print(f'STAGE 6: Verify offer URLs')
	print(f'**********************************')
	status_df = test_offers_pages(dict_pages)
	status_df.drop_duplicates(subset=['execution_date','offer_id'], keep='first', inplace=True)
	

	stemp_3 = time.time()
	print(f'\n Stage 3 execution time of pages saving:{(stemp_3 - stemp_2)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')


	# # Saving all pages to the Azure SQL database
	print(f'**********************************')
	print(f'STAGE 4: Saving results to Azure')
	print(f'**********************************')

	# Append the status of offers with today verification
	status_df.to_sql(TABLE_STAT,
				engn, 
				if_exists='append',
				method='multi',
				chunksize=100, # magic number
				dtype={'execution_date': sqlalchemy.Date(), 
					'offer_id': sqlalchemy.types.UnicodeText(),
					'offer_url': sqlalchemy.types.UnicodeText(),
					'offer_status': sqlalchemy.types.Integer()},
			index=False)



	stemp_4 = time.time()
	print(f'\n Stage 4 execution time of pages saving:{(stemp_4 - stemp_3)}')
	print(f'\n Total execution time of the script:{(time.time() - start_time)}\n')



# Execure while starting the script.py file
if __name__== '__main__':      
	main()


