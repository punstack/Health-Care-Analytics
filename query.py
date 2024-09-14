import pandas as pd
from sqlalchemy import text
from data import configure_db
import re

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
    df = preprocessing(query) # do I want to include people aged >79 in this first analysis?
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
                a.diagnosis,
                a.hospital_expire_flag
            FROM
                admissions AS a;
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

'''
old code:

RANK() OVER (PARTITION BY i.subject_id ORDER BY i.intime) AS icustay_id_order,
                CASE WHEN a.dischtime IS NULL THEN 0
                    ELSE (SELECT COUNT(*)
                        FROM admissions AS a2
                        WHERE a2.subject_id = p.subject_id
                            AND a2.admittime > a.dischtime
                            AND TIMESTAMPDIFF(DAY, a.dischtime, a2.admittime) <= 30)
                END AS readmission_within_30_days,
'''

def mapped_diagnosis(df_column):
    diagnosis_mapping = {
        r'SEPSIS': 'Infections',
        r'HEPATITIS': 'Infections',
        r'\bFRACTURE\b': 'Trauma/Injury',
        r'TRAUMA': 'Trauma/Injury',
        r'ALCOHOLIC HEPATITIS': 'Infections',
        r'STROKE': 'Neurological Issues',
        r'TIA': 'Neurological Issues',
        r'REGURGITATION': 'Cardiovascular Issues',
        r'CORONARY ARTERY DISEASE': 'Cardiovascular Issues',
        r'SYNCOPE': 'Cardiovascular Issues',
        r'RENAL': 'Renal Issues',
        r'RESPIRATORY': 'Respiratory Issues',
        r'ANGINA': 'Cardiovascular Issues',
        r'CANCER': 'Cancer',
        r'GI BLEED': 'Gastrointestinal Issues',
        r'BLEED': 'Gastrointestinal Issues',
        r'HEART FAILURE': 'Cardiovascular Issues',
        r'PNEUMONIA': 'Respiratory Issues',
        r'SEIZURE': 'Neurological Issues',
        r'LIVER': 'Liver Issues',
        r'INTRACRANIAL HEMORRHAGE': 'Neurological Issues',
        r'MYOCARDIAL INFARCTION': 'Cardiovascular Issues',
        r'STATUS POST MOTOR VEHICLE ACCIDENT': 'Trauma/Injury',
        r'UTI': 'Infections',
        r'URINARY TRACT': 'Infections',
        r'CHOLECYSTITIS': 'Gastrointestinal Issues',
        r'NEWBORN': 'Neonatal Care',
        r'ADBOMINAL': 'Gastrointestinal Issues',
        r'GASTROINTESTINAL': 'Gastointestinal Issues',
        r'UPPER GI': 'Gastrointestinal Issues',
        r'LOWER GI': 'Gastrointestinal Issues',
        r'GI BLEED': 'Gastrointestinal Issues',
        r'CORONAY ARTERY DISEASE': 'Cardiovascular Issues',
        r'INTRACRANIAL': 'Neurological Issues',
        r'SHORTNESS OF BREATH': 'Respiratory Issues',
        r'ASTHMA': 'Respiratory Issues',
        r'DYSPNEA': 'Respiratory Issues',
        r'RESPIRATORY': 'Respiratory Issues',
        r'CORONARY': 'Cardiovascular Issues',
        r'SUBARACHNOID': 'Neurological Issues',
        r'PANCREATITIS': 'Gastrointestinal Issues',
        r'HYPOXIA': 'Respiratory Issues',
        r'SUBDURAL HEMATOMA': 'Neurological Issues',
        r'CHEST': 'Respiratory Issues',
        r'MYOCARDIAL INFARCTION': 'Cardiovascular Issues',
        r'FALL': 'Trauma/Injury',
        r'KETOACIDOSIS': 'Blood Conditions',
        r'HYPOTENSION': 'Blood Conditions',
        r'HYPERTENSION': 'Blood Conditions',
        r'HYPERTENSIVE': 'Blood Conditions',
        r'ANEMIA': 'Blood Conditions',
        r'KALEMIA': 'Blood Conditions',
        r'NATREMIA': 'Blood Conditions',
        r'CAROTID': 'Cardiovascular Issues',
        r'PULMONARY': 'Respiratory Issues',
        r'UROSEPSIS': 'Infections',
        r'\w*MENTAL STATUS': 'Neurological Issues',
        r'ABDOMINAL': 'Gastrointestinal Issues',
        r'CELLULITIS': 'Infections',
        r'MOTOR VEHICLE ACCIDENT': 'Trauma/Injury',
        r'BOWEL OBSTRUCTION': 'Gastrointestinal Issues',
        r'COPD': 'Respiratory Issues',
        r'BRADYCARDIA': 'Respiratory Issues',
        r'HYPOGLYCEMIA': 'Blood Conditions',
        r'CARDIAC': 'Cardiovascular Issues',
        r'AORTIC': 'Cardiovascular Issues',
        r'HEMOPYSIS': 'Respiratory Issues',
        r'CHOLANGITIS': 'Gastrointestinal Issues',
        r'STEMI': 'Cardiovascular Issues',
        r'TUMOR': 'Cancer',
        r'PERICARDIAL': 'Cardiovascular Issues',
        r'PLEURAL': 'Respiratory Issues',
        r'BRAIN MASS': 'Cancer',
        r'PREMATURITY': 'Neonatal Care',
        r'GASTROPARESIS': 'Gastrointestinal Issues',
        r'TACHYCARDIA': 'Cardiovascular Issues',
        r'HEPATIC': 'Liver Issues',
        r'SUBDURAL': 'Neurological Issue',
        r'FAILURE TO THRIVE': 'Neonatal Care',
        r'CEREBRAL': 'Neurological Issues',
        r'ENDOCARDITIS': 'Cardiovascular Issues',
        r'INFECTION': 'Infections',
        r'BACTEREMIA': 'Infections',
        r'INTRAPARENCHYMAL': 'Neurological Issues',
        r'HYPERBILIRUBINEMIA': 'Blood Conditions',
        r'ENCEPHALOPATHY': 'Neurological Issues',
        r'CEREBROVASCULAR': 'Neurological Issues',
        r'CA/SDA': 'Cancer',
        r'CHOLANGIOPANCREATOGRAPHY': 'Gastrointestinal Issues'
    }

    def categorize_diagnosis(diagnosis):
        if pd.isna(diagnosis) or not isinstance(diagnosis, str):
            return 'Other'
        
        for pattern, category in diagnosis_mapping.items():
            if re.search(pattern, diagnosis, re.IGNORECASE):
                return category
        return 'Other'

    df_transformed = df_column.apply(categorize_diagnosis)
    return df_transformed

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