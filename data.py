import pandas as pd
from sqlalchemy import create_engine, text
import configparser
import os

def configure_db():
    '''
        connect to the MySQL database using the config.ini credentials
    '''
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
    return engine

def chunk_processing(engine, csv_file):
    chunksize = 5000
    for i, chunk in enumerate(pd.read_csv(f"mimic-iii-clinical-database-1.4/{csv_file}.csv.gz", chunksize = chunksize, low_memory = False)):
        try:
            chunk.to_sql(csv_file.lower(), con = engine, if_exists = 'append', index = False)
            print(f"{csv_file} chunk {i+1} was uploaded to SQL database successfully.")
        except Exception as e:
            print(f"Error while uploading {csv_file} chunk {i+1}: {e}")
            break

def chartevents_db(engine):
    '''
        writing CHARTEVENTS.csv to a SQL file of the same name; had processing errors before
    '''    
    dtype = {
        'value': 'str',
        'valueuom': 'str',
        'resultstatus': 'str',
        'stopped': 'str'
    }

    chunksize = 5000
    for i, chunk in enumerate(pd.read_csv("mimic-iii-clinical-database-1.4/CHARTEVENTS.csv.gz", dtype = dtype, chunksize = chunksize, low_memory = False)):
        try:
            chunk.to_sql("chartevents", con=engine, if_exists='append', index=False)
            print(f"CHARTEVENTS chunk {i+1} was uploaded to SQL database successfully.")
        except Exception as e:
            print(f"Error while uploading CHARTEVENTS chunk {i+1}: {e}")
            break

def update_db(csv_file_names:list, engine):
    '''
        load and write data to an SQL table with the same name as the CSV file in MIMIC III
    '''
    for csv_file in csv_file_names:
        if csv_file == "CHARTEVENTS":
            chartevents_db(engine)
            continue

        chunk_processing(engine, csv_file)

def upload_db():
    '''
        stacks previous functions for readability
    '''
    engine = configure_db()
    update_db(["ADMISSIONS",
                "CALLOUT",
                "CAREGIVERS",
                "CHARTEVENTS",
                "CPTEVENTS",
                "D_CPT",
                "D_ICD_DIAGNOSES",
                "D_ICD_PROCEDURES",
                "D_ITEMS",
                "D_LABITEMS",
                "DATETIMEEVENTS",
                "DIAGNOSES_ICD",
                "DRGCODES",
                "ICUSTAYS",
                "INPUTEVENTS_CV",
                "INPUTEVENTS_MV",
                "LABEVENTS",
                "MICROBIOLOGYEVENTS",
                "NOTEEVENTS",
                "OUTPUTEVENTS",
                "PATIENTS",
                "PRESCRIPTIONS",
                "PROCEDUREEVENTS_MV",
                "PROCEDURES_ICD",
                "SERVICES",
                "TRANSFERS"],
                engine)
    engine.dispose()

if __name__ == "__main__":
    upload_db()