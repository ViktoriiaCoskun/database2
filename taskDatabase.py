from sqlalchemy import create_engine, MetaData, Integer, String, Table, Column,DateTime,Float
import csv 

engine = create_engine('sqlite:///databaseTask.db', echo=True)

meta = MetaData()

clean_stations = Table(
   'clean_stations', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', String),
   Column('longitude', String),
   Column('elevation', String),
   Column('name', String),
   Column('country', String),
   Column('state', String),
)

clean_measure = Table(
   'clean_measure', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('date', String),
   Column('precip', String),
   Column('tobs', Integer),
)
meta.create_all(engine)

def store_data(filePath):
    data=[]
    with open(filePath,"r") as csv_file:
        reader = csv.DictReader(csv_file)
        for item in reader:
            data.append(dict(item))
    return data

def store_table(data,table):
    ins = table.insert()
    conn = engine.connect()
    conn.execute(ins, data)

clean_measure_data=store_data("clean_measure.csv")
store_table(clean_measure_data,clean_measure)

clean_stations_data=store_data("clean_stations.csv")
store_table(clean_stations_data,clean_stations)

conn = engine.connect()

result=conn.execute("SELECT * FROM clean_stations LIMIT 5").fetchall()
for row in result:
   print(row)

