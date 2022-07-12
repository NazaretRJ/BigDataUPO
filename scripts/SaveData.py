import happybase
import sys
import csv

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
    connection.delete_table('sensorsdb', True) 

connection.create_table('sensorsdb', families)

table = connection.table('sensorsdb')

#print(connection.tables()) 

with open('test.csv') as dataset_file:
    id = 0 
    dataset_file = csv.reader(dataset_file, delimiter=',')
    for row_dataset in dataset_file:
        for j in range(rows):
            row_table = {   'idTimecf:col1' : str(j) + row_dataset[0],
                            'idTimecf:col2' : row_dataset[1] }
            for i in range(colums):
                name = 'measurecf' + str(i) + ":col1"
                row_table[name] = row_dataset[2]

            for key, value in row_table.items():
                print(key, ' : ', value)
     
            print("---------------------")
            table.put(str(id), row_table)
            id = id +1
            

