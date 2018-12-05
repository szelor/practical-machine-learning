import pyodbc
import pickle
import os

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER={MS};DATABASE={ML};Trusted_Connection=yes;')
cursor = cnxn.cursor()
cursor.execute("EXECUTE [NYCTaxi].[SerializePlots]")
tables = cursor.fetchall()
for i, table in enumerate(tables):
    fig = pickle.loads(table[0])
    fig.savefig(str(i)+'.png')

print("The plots are saved in directory:", os.getcwd())
