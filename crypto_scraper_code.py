from bs4 import BeautifulSoup
#import os
import requests
import time
import datetime
import pandas as pd
import s3fs 
	

def main():
    
    #dag path
	#dag_path = os.getcwd()
    
	url = 'https://crypto.com/price?page='
	
	allRecordsCombined = []
	
	for page in range(1,3):
		
		# Make a request to the website
		response = requests.get(url+str(page))
		current_timestamp = datetime.datetime.now()
	
		soup = BeautifulSoup(response.content, 'html.parser')

		# Find the table containing the top 100 cryptocurrencies
		treeTag = soup.find_all('tr')

		#print(treeTag)

		
		for tree in treeTag[1:]:
			rank = tree.find('td',{'class': 'css-w6jew4'}).get_text()
			name = tree.find('p',{'class': 'chakra-text css-rkws3'}).get_text()
			symbol = tree.find('span',{'class': 'css-1jj7b1a'}).get_text()
			market_cap = tree.find('td',{'class':'css-1nh9lk8'}).get_text()
			change_24h = ""
			price_arr = str(tree.find('div',{'class':'css-16q9pr7'}).get_text())
			if('-' in price_arr):
				price_arr = price_arr.split('-')
				change_24h = '-'+price_arr[1]
			else:
				price_arr = price_arr.split('+')
				change_24h = '+'+price_arr[1]
			price = price_arr[0]
			volume_24 = tree.find('td',{'class':'css-1nh9lk8'}).get_text()
			

			#print("Rank: ", rank)
			#print("NAME: ", name)
			#print("symbol: ", symbol)
			#print("price: ", price)
			#print("market_cap: ", market_cap)
			#print("volume_24: ", volume_24)
			#print("change_24h: ", change_24h)
			
			allRecordsCombined.append([current_timestamp, rank, name, symbol, price, change_24h, volume_24, market_cap])
		
	columns = ['SYSTEM_INSERTED_TIMESTAMP', 'RANK','NAME', 'SYMBOL', 'PRICE', 'PERCENT_CHANGE_24H','VOLUME_24H', 'MARKET_CAP']
	df = pd.DataFrame(columns=columns, data=allRecordsCombined)
	current_timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
	df.to_csv('s3://bucket_name/raw_layer/{}.csv'.format(current_timestamp), index=False)
	#df.to_csv(f"{dag_path}/dags/output/{current_timestamp}.csv", index=False)
	#print(f"FILE created at: {dag_path}/output/{current_timestamp}.csv")
	
