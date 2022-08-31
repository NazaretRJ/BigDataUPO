import ast
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
  
try:
    if(connection.is_table_enabled('sensorsdb')):
        print("The table is alredy created, so it is going to be deleted")
        connection.delete_table('sensorsdb', True)
        connection.create_table('sensorsdb', families)
        
except Exception as e:
    print("The table doesn't exist, so it is going to be created")
    connection.create_table('sensorsdb', families)

table = connection.table('sensorsdb')

print("Reading data...")

try:
    with open('testCorto.csv') as dataset_file:
        dataset_file = csv.reader(dataset_file, delimiter=',')
        for row_dataset in dataset_file:
            for j in range(rows):
                row_table = {'idTimecf:col1' : str(j) + row_dataset[0],
                             'idTimecf:col2' : row_dataset[1] }
                for i in range(colums):
                    name = 'measurecf' + str(i) + ":col1"
                    row_table[name] = row_dataset[2]

                timestamp = time.mktime(datetime.datetime.strptime(row_dataset[1], "%Y-%m-%d %H:%M").timetuple())
                table.put( str(j) + "-" + row_dataset[0] + str(int(timestamp)), row_table, int(timestamp))

except Exception as e:
    print("Error occurs while It was reading data")
                     
print("Please insert the number of the measure colum you want to extract (C). Starts at 0: ")
measureColum = int(input())

print("Please insert the number of the sensor identification  you want to extract(F). Starts at 0: ")
idsensor = int(input())

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
      
except Exception as e:
    print("Exception")
    print(e)