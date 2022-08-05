from ast import Str
from sqlite3 import Timestamp
import happybase
import sys
import csv
import time
import datetime

connection = happybase.Connection('localhost')

connection.open()

print("Please insert the number of times you want to repeat the colums (C) : ")
colums = int(input())

print("Please insert the number of times you want to repeat the rows (F) : ")
rows = int(input())

if(colums <= 0 or rows <= 0):
    print("The inputs are not valid")
    sys.exit()
    
families = {
    'idTimecf' : dict(max_versions=1)     
}

for i in range(colums):
    name = 'measurecf' + str(i)
    families[name] = dict(max_versions=1)
    
if( connection.is_table_enabled('sensorsdb')):
    print("The table is alredy created, so it is going to be deleted")
    connection.delete_table('sensorsdb', True) 

connection.create_table('sensorsdb', families)

table = connection.table('sensorsdb')

with open('test.csv') as dataset_file:
    dataset_file = csv.reader(dataset_file, delimiter=',')
    for row_dataset in dataset_file:
        for j in range(rows):
            row_table = {   'idTimecf:col1' : str(j) + row_dataset[0],
                            'idTimecf:col2' : row_dataset[1] }
            for i in range(colums):
                name = 'measurecf' + str(i) + ":col1"
                row_table[name] = row_dataset[2]

            timestamp = time.mktime(datetime.datetime.strptime(row_dataset[1], "%Y-%m-%d %H:%M").timetuple())
            print(str(j) + "-" + row_dataset[0] + "-" + str(int(timestamp)))
            table.put( str(j) + "-" + row_dataset[0] + str(int(timestamp)), row_table, int(timestamp))
           
print("Please insert the number of the measure colum you want to extract (C) : ")
measureColum = int(input())

print("Please insert the number of the sensor identification  you want to extract(F) : ")
idsensor = int(input())

try:
    for key, data in table.scan(row_prefix=b"%d" % idsensor, columns=['idTimecf:col1', 'idTimecf:col2','measurecf'+str(measureColum)+':col1']):
        print(key, data)
except Exception as e:
    print(e)