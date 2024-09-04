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

def update_db(csv_file_names:list, engine):
    '''
        load and write data to an SQL table with the same name as the CSV file in MIMIC III
    '''
    for csv_file in csv_file_names:
        if csv_file == "CHARTEVENTS":
            chartevents_db(engine)
            continue

        data = pd.read_csv(f"mimic-iii-clinical-database-demo-1.4/{csv_file}.csv")
        try:
            data.to_sql(csv_file.lower(), con=engine, if_exists='replace', index=False)
            print(f"{csv_file} was uploaded to SQL database successfully.")
        except:
            print(f"{csv_file} could not be uploaded to the database.")

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

    chunksize = 50000
    for chunk in pd.read_csv("mimic-iii-clinical-database-demo-1.4/CHARTEVENTS.csv", dtype = dtype, chunksize = chunksize):
        try:
            chunk.to_sql("chartevents", con=engine, if_exists='append', index=False)
            print("CHARTEVENTS chunk was uploaded to SQL database successfully.")
        except:
            print("CHARTEVENTS chunk could not be uploaded to the database.")
            engine.dispose()

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

if __name__ == "__main__":
    # testing upload of all tables
    engine = configure_db()
    
    with engine.connect() as connection:
        query = text("SELECT row_id FROM chartevents LIMIT 5")
        result = connection.execute(query)
        for row in result:
            print(row)