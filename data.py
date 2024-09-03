import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import os

config = configparser.ConfigParser()

config_file = 'config.ini'
if not os.path.isfile(config_file):
    raise FileNotFoundError(f"The configuration file {config_file} does not exist.")

config.read(config_file)

host = config['mysql']['host']
user = config['mysql']['user']
password = config['mysql']['password']
database = config['mysql']['database']

connection_string = f'mysql+mysqlconnector://{user}:{password}@{host}/{database}'
engine = create_engine(connection_string)

data = pd.read_csv("mimic-iii-clinical-database-demo-1.4/ADMISSIONS.csv")

data.to_sql('admissions', con=engine, if_exists='replace', index=False)

with engine.connect() as connection:
    query = text("SELECT row_id AS id FROM admissions LIMIT 5")
    result = connection.execute(query)
    for row in result:
        print(row)
