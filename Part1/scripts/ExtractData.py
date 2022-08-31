import ast
from sqlite3 import Timestamp
import happybase
import sys
import csv
import time
import datetime
from pathlib import Path

connection = happybase.Connection('localhost')

connection.open()

try:
    if(connection.is_table_enabled('sensorsdb') != True):
        print("The table doesn't exist. Please use the other script")
        sys.exit()
    
    table = connection.table('sensorsdb')
    
except Exception as e:
    print("An error occurs while trying to detec the table")


                  
print("Please insert the number of the measure colum you want to extract (C). Starts at 0: ")
measureColum = int(input())

print("Please insert the number of the sensor identification you want to extract(F). Starts at 0: ")
idsensor = int(input())

if(measureColum < 0 or idsensor < 0):
    print("The inputs are not valid")
    sys.exit()
    
try:
    header = ['Sensor','Date']
    for hour in range(0, 24):
        for min in range(0, 51, 10):
            header.append(str("{0:02d}".format(hour)) + ":" + str("{0:02d}".format(min)))
            
    with open('results.csv', 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        data_row = []
        for key, data in table.scan(row_prefix=b"%d" % idsensor, columns=['idTimecf:col1', 'idTimecf:col2','measurecf'+str(measureColum)+':col1']):
            
            hour = data[b"idTimecf:col2"].decode("utf-8")
            hour = hour[11:]

            if(hour == "23:50"):
                id = data[b"idTimecf:col1"].decode("utf-8").replace(str(idsensor),"",1)
                data_row.insert(0, id)
                
                timestamp = data[b"idTimecf:col2"].decode("utf-8")
                timestamp = timestamp[:-6]
                data_row.insert(1, timestamp)
                
                colum_name = 'measurecf'+str(measureColum)+':col1'
                data_row.append(data[colum_name.encode('utf-8')].decode("utf-8"))
            
                writer.writerow(data_row)
                data_row = []
                lastTime = timestamp
            else:
                colum_name = 'measurecf'+str(measureColum)+':col1'
                data_row.append(data[colum_name.encode('utf-8')].decode("utf-8"))
    print("Done! Review the results.csv")
except Exception as e:
    print("Error!")
    print("Make sure that the table has enough colums to save your data." )
    print("For example, if the table just have 3 colums for value, you need to create again the table or change the number you want to extract.")