import pandas as pd
from sqlalchemy import text
from data import configure_db

def preprocessing(query):
    engine = configure_db()
    return pd.read_sql(query, con = engine)

def query_a():
    query = text("""
            SELECT
                p.gender,
                TIMESTAMPDIFF(YEAR, p.dob, p.dod) AS age,
                i.los,
                ABS(TIMESTAMPDIFF(DAY, p.dod, a.dischtime)) AS death_days,
                IF(ABS(TIMESTAMPDIFF(DAY, p.dod, a.dischtime)) > 90, "death not within 90 days of discharge",
                    IF(TIMESTAMPDIFF(DAY, p.dod, a.dischtime) = 0, "death within hospital",
                        "death within 90 days of discharge")) AS death_time,
                a.hospital_expire_flag
            FROM
                patients AS p
            INNER JOIN
                icustays AS i   
                ON p.subject_id = i.subject_id
            INNER JOIN
                admissions AS a
                ON p.subject_id = a.subject_id
            WHERE 
               TIMESTAMPDIFF(YEAR, p.dob, p.dod) < 300;
            """)
    df = preprocessing(query)
    return df

def query_b():
    query = text("""
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
                IF(ABS(TIMESTAMPDIFF(DAY, p.dod, a.dischtime)) > 90, "death not within 90 days of discharge",
                    IF(TIMESTAMPDIFF(DAY, p.dod, a.dischtime) = 0, "death within hospital",
                        "death within 90 days of discharge")) AS death_time,
                a.hospital_expire_flag
            FROM
                admissions AS a
            INNER JOIN
                patients AS p
                ON a.subject_id = p.subject_id
            INNER JOIN
                icustays AS i
                ON a.subject_id = i.subject_id;
            """)
    df = preprocessing(query)
    return df

def query_c():
    query = text("""
            SELECT
                a.admittime,
                a.insurance,
                a.religion,
                a.marital_status,
                a.ethnicity,
                a.diagnosis
            FROM
                admissions AS a;
            """)
    df = preprocessing(query)
    return df

def query_d():
    query = text("""
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
            INNER JOIN
                chartevents AS c
                ON a.hadm_id = c.hadm_id
            INNER JOIN
                microbiologyevents AS m
                ON a.hadm_id = m.hadm_id
            INNER JOIN
                d_items AS d
                ON c.itemid = d.itemid
            WHERE
                LOWER(a.diagnosis) LIKE '%sepsis%';
            """)
    df = preprocessing(query)
    return df

def query_e():
    # TODO: check if subject id, hadm id are necessary for analysis
    # TODO: change logic of this query to look for multiple "admit" transfer_type for the same subject_id
    query = text("""
            SELECT
                p.subject_id,
                p.gender,
                TIMESTAMPDIFF(YEAR, p.dob, a.admittime) AS age,
                a.hadm_id,
                a.admission_type,
                a.admittime,
                a.dischtime,
                a.hospital_expire_flag,
                a.diagnosis,
                i.los,
                s.curr_service,
                t.eventtype AS transfer_type,
                t.intime AS transfer_intime,
                t.outtime AS transfer_outtime,
                (SELECT COUNT(*)
                FROM admissions AS a2
                WHERE a2.subject_id = p.subject_id
                    AND a2.admittime > a.dischtime
                    AND TIMESTAMPDIFF(DAY, a.dischtime, a2.admittime) <= 30) AS readmission_within_30_days
            FROM
                patients AS p
            INNER JOIN
                admissions AS a
                ON p.subject_id = a.subject_id
            INNER JOIN
                icustays AS i
                ON a.hadm_id = i.hadm_id
            INNER JOIN
                services AS s
                ON a.hadm_id = s.hadm_id
            LEFT JOIN
                transfers AS t
                ON a.hadm_id = t.hadm_id
            WHERE
                a.hospital_expire_flag = 0;
            """)
    df = preprocessing(query)
    return df