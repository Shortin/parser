import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re 
from config import connection_to_DB
# import time


def main():
	connection = connection_to_DB()
	if(connection == False):
		print("Проверь ip")
		return
	cursor = connection.cursor()
	nameTable = "muztorg"
	drop_table(cursor, connection, nameTable)

	if(create_table(cursor, connection, nameTable)):
		print("Table was created successfully")
	else:
		print("Table was created successfully")
		return
	if(pars(cursor, connection, nameTable)):
		print("parsing was successfully")
	else:
		print("parsing was not successfully")
	cursor.close()
	connection.close() 

def create_table(cursor, connection, nameTable):
	try:
		cursor.execute(f"CREATE TABLE {nameTable} (id SERIAL PRIMARY KEY, name VARCHAR(100),  price INTEGER)")
		# поддверждаем транзакцию
		connection.commit()
		return True
	except:
		return False

def drop_table(cursor, connection, nameTable):
	try:
		cursor.execute(f"DROP TABLE {nameTable}")
		# поддверждаем транзакцию
		connection.commit()
		return True
	except:
		return False


def insert_data(cursor, connection, nameTable, name, price):
	try:
		# добавляем строку в таблицу people
		cursor.execute(f"INSERT INTO {nameTable} (name, price) VALUES ('{name}', {price})")
		# выполняем транзакцию
		connection.commit()  
		return True
	except:
		return False

def pars(cursor, connection, nameTable):
	number = 1
	while(True):
		URL_TEMPLATE = "https://www.muztorg.ru/category/akusticheskie-gitary?in-stock=1&pre-order=1&page=" + str(number)
		r = requests.get(URL_TEMPLATE)
		soup = bs(r.text, "html.parser")
		names = soup.find_all('div', class_='title')
		price = soup.find_all('meta', itemprop='price')

		check = 0
		trans_table = {ord('\"') : None, ord('\'') : None, ord('\n') : None}
		for i in range(len(names)):
			nameData = names[i].text.strip().translate(trans_table)
			priceData = int(re.split(r'"', str(price[i]))[1])

			if(insert_data(cursor, connection, nameTable, nameData, priceData) == False):
				print(f"Erorr i = {i}, name = {nameData}, price = {priceData}")
				return False
			check += 1
		if(check == 0):
			return True
		else:
			number += 1
			print(number)




main()

