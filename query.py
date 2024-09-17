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
                TIMESTAMPDIFF(YEAR, p.dob, a.admittime) AS age,
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
               TIMESTAMPDIFF(YEAR, p.dob, p.dob) < 300;
            """)
    df = preprocessing(query)
    return df

def query_b():
    query = text("""
            SELECT
                p.gender,
                TIMESTAMPDIFF(YEAR, p.dob, a.admittime) AS age,
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
                patients AS p
            INNER JOIN
                admissions AS a
                ON a.subject_id = p.subject_id
            INNER JOIN
                icustays AS i
                ON a.subject_id = i.subject_id
            WHERE 
               TIMESTAMPDIFF(YEAR, p.dob, a.admittime) < 300;
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
                a.diagnosis,
                a.hospital_expire_flag
            FROM
                admissions AS a;
            WHERE
               TIMESTAMPDIFF(YEAR, p.dob, a.admittime) < 300;
            """)
    df = preprocessing(query)
    return df

def query_d():
    query = text("""
            SELECT
                p.gender,
                TIMESTAMPDIFF(YEAR, p.dob, a.admittime) AS age,
                a.admission_type,
                a.admittime,
                a.dischtime,
                a.diagnosis,
                i.los,
                CASE WHEN a.dischtime IS NULL
                    OR (SELECT COUNT(*)
                        FROM admissions AS a2
                        WHERE a2.subject_id = p.subject_id
                            AND a2.admittime > a.dischtime
                            AND TIMESTAMPDIFF(DAY, a.dischtime, a2.admittime) <= 30) > 0
                    THEN 1
                    ELSE 0
                END AS readmission_flag
            FROM
                patients AS p
            INNER JOIN
                admissions AS a
                ON p.subject_id = a.subject_id
            INNER JOIN
                icustays AS i
                ON a.hadm_id = i.hadm_id  
            WHERE
                a.hospital_expire_flag = "0"
            """)
    df = preprocessing(query)
    return df

def mapped_ethnicies(df_column):
    ethnicity_mapping = {
        'HISPANIC OR LATINO': 'HISPANIC/LATINO',
        'WHITE': 'WHITE',
        'UNKNOWN/NOT SPECIFIED': 'UNKNOWN/UNSPECIFIED',
        'PATIENT DECLINED TO ANSWER': 'UNKNOWN/UNSPECIFIED',
        'BLACK/AFRICAN AMERICAN': 'BLACK/AFRICAN AMERICAN',
        'ASIAN': 'ASIAN',
        'OTHER': 'UNKNOWN/UNSPECIFIED',
        'ASIAN - VIETNAMESE': 'ASIAN',
        'HISPANIC/LATINO - GUATEMALAN': 'HISPANIC/LATINO',
        'ASIAN - CHINESE': 'ASIAN',
        'HISPANIC/LATINO - PUERTO RICAN': 'HISPANIC/LATINO',
        'ASIAN - ASIAN INDIAN': 'ASIAN',
        'MULTI RACE ETHNICITY': 'MULTIRACIAL',
        'HISPANIC/LATINO - DOMINICAN': 'HISPANIC/LATINO',
        'AMERICAN INDIAN/ALASKA NATIVE': 'AMERICAN INDIAN/ALASKA NATIVE',
        'WHITE - RUSSIAN': 'WHITE',
        'BLACK/AFRICAN': 'BLACK/AFRICAN AMERICAN',
        'HISPANIC/LATINO - SALVADORAN': 'HISPANIC/LATINO',
        'UNABLE TO OBTAIN': 'UNKNOWN/UNSPECIFIED',
        'HISPANIC/LATINO - CENTRAL AMERICAN (OTHER)': 'HISPANIC/LATINO',
        'ASIAN - JAPANESE': 'ASIAN',
        'WHITE - OTHER EUROPEAN': 'WHITE',
        'HISPANIC/LATINO - CUBAN': 'HISPANIC/LATINO',
        'BLACK/HAITIAN': 'BLACK/AFRICAN AMERICAN',
        'ASIAN - CAMBODIAN': 'ASIAN',
        'HISPANIC/LATINO - COLOMBIAN': 'HISPANIC/LATINO',
        'BLACK/CAPE VERDEAN': 'BLACK/AFRICAN AMERICAN',
        'MIDDLE EASTERN': 'MIDDLE EASTERN',
        'WHITE - BRAZILIAN': 'WHITE',
        'WHITE - EASTERN EUROPEAN': 'WHITE',
        'CARIBBEAN ISLAND': 'BLACK/AFRICAN AMERICAN',
        'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER': 'NATIVE HAWAIIAN OR OTHER PACIFIC ISLANDER',
        'ASIAN - FILIPINO': 'ASIAN',
        'ASIAN - KOREAN': 'ASIAN',
        'AMERICAN INDIAN/ALASKA NATIVE FEDERALLY RECOGNIZED TRIBE': 'AMERICAN INDIAN/ALASKA NATIVE',
        'SOUTH AMERICAN': 'HISPANIC/LATINO',
        'HISPANIC/LATINO - MEXICAN': 'HISPANIC/LATINO',
        'HISPANIC/LATINO - HONDURAN': 'HISPANIC/LATINO',
        'PORTUGUESE': 'HISPANIC/LATINO',
        'ASIAN - THAI': 'ASIAN',
        'ASIAN - OTHER': 'ASIAN'
    }

    df_transformed = df_column.map(ethnicity_mapping).fillna('UNKNOWN/UNSPECIFIED')

    return df_transformed

if __name__ == "__main__":
    engine = configure_db
    df_a = query_a()
    df_a.info()