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
            ABS(TIMESTAMPDIFF(DAY, p.dod, a.dischtime)) AS death_days,
            IF(ABS(TIMESTAMPDIFF(DAY, p.dod, a.dischtime)) > 90, "death not within 90 days of discharge", IF(TIMESTAMPDIFF(DAY, p.dod, a.dischtime) = 0, "death within hospital", "death within 90 days of discharge")) AS death_time,
            a.hospital_expire_flag
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
    # what are the most common admission diagnoses for different age groups?
    text("""
        SELECT
            p.gender,
            TIMESTAMPDIFF(YEAR, p.dob, p.dod) AS age,
            a.insurance,
            a.religion,
            a.marital_status,
            a.ethnicity,
            a.diagnosis,
            i.los,
            ABS(TIMESTAMPDIFF(DAY, p.dod, a.dischtime)) AS death_days,
            IF(ABS(TIMESTAMPDIFF(DAY, p.dod, a.dischtime)) > 90, "death not within 90 days of discharge", IF(TIMESTAMPDIFF(DAY, p.dod, a.dischtime) = 0, "death within hospital", "death within 90 days of discharge")) AS death_time,
            a.hospital_expire_flag
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
    # how do patient outcomes vary by season or time of admission?
    # can we observe any seasonal trends in specific diagnoses?
    text("""
        SELECT
            a.admittime,
            a.insurance,
            a.religion,
            a.marital_status,
            a.ethnicity,
            a.diagnosis
        FROM
            admissions AS a
        LIMIT 2;
    """),
    # how does the timing of antibiotic administration affect sepsis outcomes? what about the type of medication administered?

    text("""
        SELECT COUNT(*) AS mismatch_count
        FROM chartevents AS c
        INNER JOIN admissions AS a
        ON TRIM(c.hadm_id) != TRIM(a.hadm_id)
        WHERE c.hadm_id = a.hadm_id;
    """),
    text("""
        SELECT a.hadm_id AS adm_hadm_id, c.hadm_id AS chart_hadm_id
        FROM admissions AS a
        INNER JOIN chartevents AS c
        ON a.hadm_id = c.hadm_id
        LIMIT 10;
    """),
    text("""
        SELECT
            a.hadm_id,
            c.charttime,
            c.storetime,
            c.value
        FROM
            admissions AS a
        INNER JOIN
            chartevents AS c
        ON a.hadm_id = c.hadm_id
        WHERE LOWER(a.diagnosis) LIKE '%sepsis%'
        LIMIT 10;
    """),
    text("""
        SELECT
            a.hadm_id,
            c.charttime,
            c.value,
            m.org_name
        FROM
            admissions AS a
        INNER JOIN
            chartevents AS c
        ON a.hadm_id = c.hadm_id
        INNER JOIN
            microbiologyevents AS m
        ON a.hadm_id = m.hadm_id
        WHERE LOWER(a.diagnosis) LIKE '%sepsis%'
        LIMIT 10;
    """)
    # can we build a predictive model that identifies patients as being high risk for readmission?
]

'''


    text("""
        SELECT
            a.admittime,
            a.dischtime,
            a.hospital_expire_flag,
            c.charttime,
            c.storetime,
            c.value,
            c.valuenum,
            c.valueuom,
            d.label,
            d.abbreviation,
            d.category,
            d.unitname,
            m.spec_itemid,
            m.spec_type_desc,
            m.org_itemid,
            m.org_name,
            m.ab_itemid,
            m.ab_name,
            m.interpretation
        FROM
            admissions AS a
        LEFT JOIN
            chartevents AS c
            ON a.hadm_id = c.hadm_id
        LEFT JOIN
            d_items AS d
            ON c.itemid = d.itemid
        LEFT JOIN
            microbiologyevents AS m
            ON a.hadm_id = m.hadm_id
        WHERE
            LOWER(a.diagnosis) LIKE '%sepsis%'
        LIMIT 2;
    """),

'''
    

if __name__ == "__main__":
    # commented out because it has been done
    # upload_db()

    # used to connect to engine without reuploading all of the data
    # configure_db

    # engine.dispose()

    preprocessing(queries)
