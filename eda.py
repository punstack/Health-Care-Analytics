from data import configure_db, upload_db
import pandas as pd
from sqlalchemy import text

def preprocessing(queries:list):
    engine = configure_db()

    for query in queries:
        with engine.connect() as connection:
            result = connection.execute(query)
            for row in result:
                print(row)
        print("-----------------")
    

queries = [
    # NOTE: currently working with the demo data. make sure to upload to the full data when available!
    # TODO: remove limits and remove printing in "preprocessing" function; commit to pd df
    # what are the age and gender distributions of patients admitted to the ICU?
    text("""
        SELECT
            p.gender,
            TIMESTAMPDIFF(YEAR, p.dob, p.dod) AS age,
            i.los,
            TIMESTAMPDIFF(DAY, p.dod, a.dischtime) AS death_days,
            IF(TIMESTAMPDIFF(DAY, p.dod, a.dischtime) > 90, "death not within 90 days of discharge", IF(TIMESTAMPDIFF(DAY, p.dod, a.dischtime) = 0, "death within hospital", "death within 90 days of discharge")) AS death_time
        FROM
            patients AS p
        INNER JOIN
            icustays AS i   
            ON p.subject_id = i.subject_id
        INNER JOIN
            admissions AS a
            ON p.subject_id = a.subject_id
        LIMIT 2;
        """),
    # what are the demographics of patients admitted to the ICU? what are there diagnoses?
    text("""
        SELECT
            a.insurance,
            a.religion,
            a.marital_status,
            a.ethnicity,
            a.diagnosis,
            i.los,
            TIMESTAMPDIFF(DAY, p.dod, a.dischtime) AS death_days,
            IF(TIMESTAMPDIFF(DAY, p.dod, a.dischtime) > 90, "death not within 90 days of discharge", IF(TIMESTAMPDIFF(DAY, p.dod, a.dischtime) = 0, "death within hospital", "death within 90 days of discharge")) AS death_time
        FROM
            admissions AS a
        INNER JOIN
            patients AS p
            ON a.subject_id = p.subject_id
        INNER JOIN
            icustays AS i
            ON a.subject_id = i.subject_id
        LIMIT 2;
         """),
    # 
]
    

if __name__ == "__main__":
    # commented out because it has been done
    # upload_db()

    # used to connect to engine without reuploading all of the data
    # configure_db

    # engine.dispose()

    preprocessing(queries)
