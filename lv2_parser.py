import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import re 
from config import connection_to_DB	
import psycopg2 as pgsql
from psycopg2 import OperationalError
import time 

Connection = ""
Cursor = ""

def main():
	start = time.time()
	connection = connection_to_DB()
	if(connection == False):
		print("Проверь ip")
		return
	global Connection
	Connection = connection
	global Cursor
	Cursor = Connection.cursor()
	parsing_table()
	Cursor.close()
	Connection.close() 
	end = time.time() - start ## собственно время работы программы

	print(end) ## вывод времени
	
def drop_table(nameTable):
	try:
		Cursor.execute("ROLLBACK")
		Connection.commit()
		Cursor.execute(f"DROP TABLE {nameTable}")
		# поддверждаем транзакцию
		Connection.commit()
		return True
	except:
		return False

def insert_data(dbString):
	try:
		Cursor.execute("ROLLBACK")
		Connection.commit()
		Cursor.execute(dbString)
		# выполняем транзакцию
		Connection.commit() 
		return True 
	except:
		return False

def mod_Replace(string):
	oldValue = ' ','-', '(', ')', '/'
	for value in oldValue:
		string = string.replace(value, '_')
	string = string.replace(',', '')
	return string



def create_Table(nameTable, dbString):	
	nameTable = mod_Replace(nameTable)		
	drop_table(nameTable)
	# Если сюда поставить return false можно удалить созданные таблицы

	# Создание таблицы
	dbString = str(f"CREATE TABLE {nameTable} ") + dbString
	if(insert_data(dbString)):
		print(nameTable + " created successfully")
		return True
	else:
		print(nameTable + " not created successfully")
		return False
	return True

def parsing_table():
	url = "https://www.muztorg.ru"
	req = requests.get(url)
	soup = bs(req.text, "lxml")
	catalog = soup.find_all('div', "catalog-menu__i")[3:]

	for name in catalog:
		# Ищем все ссылки и убираем лищние пробелы
		dataParsing = str(name.find_all('a')).replace("  ", "")

		# Отделям сылки и имена от всего лишнего
		filterString = '<a href="/category/|">\n|</a>, <a href="/category/|</a>'
		dataParsing = re.split(filterString, str(dataParsing))

		# Создаем и проверяем имя таблицы
		# Данные для таблиц с сылками
		nameTable = re.split('>|\t', dataParsing[1])
		if(not len(nameTable) > 1):
			break
		nameTable = nameTable[1]
		nameTable = mod_Replace(nameTable)

		# Создаем таблицу с созданным именем

		dbString = "(id SERIAL PRIMARY KEY, name VARCHAR(100),  link VARCHAR(80));"
		if(not create_Table(nameTable, dbString)):
			continue
		# переменные check и i нужны для коректного заполения таблицы
		check = i = 0
		name = link = ""
		# Цыкл заполнения таблицы данными
		while(i < len(dataParsing)):			
			if(check == 0):
				link = dataParsing[i]
			elif(check == 1):
				name = mod_Replace(dataParsing[i])
				name = nameTable +"_"+name
			if(i & 1):
				check = 0
				dbString = str(f"INSERT INTO {nameTable} (name, link) VALUES ('{name}', '{link}');")
				if(not insert_data(dbString)):
					print("Не удалось заполнить таблицу " + nameTable)
				else:
					dbString = "(id SERIAL PRIMARY KEY, name VARCHAR(100),  price INTEGER);"
					if(not create_Table(name, dbString)):
						print("не удалось создать таблицу " + name)
					else:
						parsing_data(link, name)
			else:
				check += 1
			i += 1
			
	print("Прасинг произошел успешно")

def parsing_data(url, nameTable):
	nameTable = mod_Replace(nameTable)		
	number = 1	
	while(True):
		print(number)
		URL_TEMPLATE = f"https://www.muztorg.ru/category/{url}?in-stock=1&pre-order=1&page=" + str(number)
		r = requests.get(URL_TEMPLATE)
		soup = bs(r.text, "lxml")
		names = soup.find_all('div', class_='title')
		price = soup.find_all('meta', itemprop='price')

		check = 0
		for i in range(len(names)):
			try:
				nameData = names[i].text.strip().translate({ord('\"') : None, ord('\'') : None, ord('\n') : None})
				nameData = nameData[:99]
				priceData = int(re.split(r'"', str(price[i]))[1])

				dbString = str(f"INSERT INTO {nameTable} (name, price) VALUES ('{nameData}', '{priceData}');")
				if(not insert_data(dbString)):
					print(f"Erorr i = {i}, name = {nameData}, price = {priceData}")
					continue
				check += 1
			except:
				print("какая-то ошибка")

		if(check == 0):
			return True
		else:
			number += 1
		

main()
