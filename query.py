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

def mapped_diagnosis(df_column):
    diagnosis_mapping = {
        'SEPSIS': 'Infections',
        'HEPATITIS B': 'Infections',
        'HUMERAL FRACTURE': 'Trauma/Injury',
        'ALCOHOLIC HEPATITIS': 'Infections',
        'STROKE/TIA': 'Neurological Issues',
        'MITRAL REGURGITATION;CORONARY ARTERY DISEASE\\CORONARY ARTERY BYPASS GRAFT WITH MVR ? MITRAL VALVE REPLACEMENT /SDA': 'Cardiovascular Issues',
        'SYNCOPE': 'Cardiovascular Issues',
        'TELEMETRY': 'Cardiovascular Issues',
        'RIGHT HUMEROUS FRACTURE': 'Trauma/Injury',
        'RENAL FAILIURE-SYNCOPE-HYPERKALEMIA': 'Renal Issues',
        'RECURRENT LEFT CAROTID STENOSIS,PRE HYDRATION': 'Cardiovascular Issues',
        'FAILURE TO THRIVE': 'Miscellaneous',
        'PULMONARY EDEMA\\CATH': 'Respiratory Issues',
        'UNSTABLE ANGINA': 'Cardiovascular Issues',
        'RESPIRATORY DISTRESS': 'Respiratory Issues',
        'METASTATIC MELANOMA': 'Cancer',
        'BRAIN METASTASIS': 'Cancer',
        'FEVER': 'Miscellaneous',
        'BRAIN METASTASES': 'Cancer',
        'LOWER GI BLEED': 'Gastrointestinal Issues',
        'VARICEAL BLEED': 'Gastrointestinal Issues',
        'CHEST PAIN/ CATH': 'Cardiovascular Issues',
        'SUBDURAL HEMATOMA/S/P FALL': 'Trauma/Injury',
        'ESOPHAGEAL CANCER/SDA': 'Cancer',
        'S/P MOTORCYCLE ACCIDENT': 'Trauma/Injury',
        'SEIZURE': 'Neurological Issues',
        'GASTROINTESTINAL BLEED': 'Gastrointestinal Issues',
        'LUNG CANCER': 'Cancer',
        'SHORTNESS OF BREATH': 'Respiratory Issues',
        'HYPOTENSION': 'Cardiovascular Issues',
        'UROSEPSIS': 'Infections',
        'CONGESTIVE HEART FAILURE': 'Cardiovascular Issues',
        'PNEUMONIA': 'Respiratory Issues',
        'BASAL GANGLIN BLEED': 'Neurological Issues',
        'OVERDOSE': 'Miscellaneous',
        'CRITICAL AORTIC STENOSIS/HYPOTENSION': 'Cardiovascular Issues',
        'STATUS POST MOTOR VEHICLE ACCIDENT WITH INJURIES': 'Trauma/Injury',
        'TACHYPNEA': 'Respiratory Issues',
        'CHRONIC MYELOGENOUS LEUKEMIA': 'Cancer',
        'TRANSFUSION REACTION': 'Miscellaneous',
        'HYPONATREMIA': 'Renal Issues',
        'URINARY TRACT INFECTION': 'Infections',
        'HEADACHE': 'Neurological Issues',
        'VF ARREST ': 'Cardiovascular Issues',
        'PULMONARY EDEMA, MI': 'Cardiovascular Issues',
        'ACUTE CHOLECYSTITIS': 'Gastrointestinal Issues',
        'LIVER FAILURE': 'Liver Issues',
        'LEFT HIP FRACTURE': 'Trauma/Injury',
        'S/P MOTOR VEHICLE ACCIDENT': 'Trauma/Injury',
        'ABSCESS': 'Infections',
        'NON SMALL CELL CANCER': 'Cancer',
        'HYPOXIA': 'Respiratory Issues',
        'ACUTE CHOLANGITIS': 'Gastrointestinal Issues',
        'INTRACRANIAL HEMORRHAGE': 'Neurological Issues',
        'LEFT HIP OA/SDA': 'Trauma/Injury',
        'MEDIASTINAL ADENOPATHY': 'Cancer',
        'AROMEGLEY': 'Miscellaneous',
        'BURKITTS LYMPHOMA': 'Cancer',
        'FACIAL NUMBNESS': 'Neurological Issues',
        'STEMI': 'Cardiovascular Issues',
        'TRACHEAL ESOPHAGEAL FISTULA': 'Respiratory Issues',
        'CHOLECYSTITIS': 'Gastrointestinal Issues',
        'CELLULITIS': 'Infections',
        'ABDOMINAL PAIN': 'Gastrointestinal Issues',
        'ASTHMA': 'Respiratory Issues',
        'CHRONIC OBST PULM DISEASE': 'Respiratory Issues',
        'ELEVATED LIVER FUNCTIONS': 'Liver Issues',
        'S/P LIVER TRANSPLANT': 'Liver Issues',
        'UTI/PYELONEPHRITIS': 'Infections',
        'UTI': 'Infections',
        'PYELONEPHRITIS': 'Infections',
        'UNRESPONSIVE': 'Neurological Issues',
        'S/P FALL': 'Trauma/Injury',
        'TRACHEAL STENOSIS': 'Respiratory Issues',
        'INFERIOR MYOCARDIAL INFARCTION\\CATH': 'Cardiovascular Issues',
        'HEPATIC ENCEP': 'Liver Issues',
        'CHEST PAIN': 'Cardiovascular Issues',
        'RENAL CANCER/SDA': 'Cancer',
        'UPPER GI BLEED': 'Gastrointestinal Issues',
        'PNEUMONIA/HYPOGLCEMIA/SYNCOPE': 'Respiratory Issues',
        'ASTHMA/COPD FLARE': 'Respiratory Issues',
        'VOLVULUS': 'Gastrointestinal Issues',
        'STATUS EPILEPTICUS': 'Neurological Issues',
        'HYPOGLYCEMIA': 'Miscellaneous',
        'CEREBROVASCULAR ACCIDENT': 'Neurological Issues',
        'METASTIC MELANOMA': 'Cancer',
        'ANEMIA': 'Miscellaneous',
        'HYPOTENSION, RENAL FAILURE': 'Cardiovascular Issues',
        'ACUTE SUBDURAL HEMATOMA': 'Trauma/Injury',
        'ESOPHAGEAL CA/SDA': 'Cancer',
        'MI CHF': 'Cardiovascular Issues',
        'PLEURAL EFFUSION': 'Respiratory Issues',
        'ACUTE PULMONARY EMBOLISM': 'Respiratory Issues',
        'CORONARY ARTERY DISEASE\\CORONARY ARTERY BYPASS GRAFT /SDA': 'Cardiovascular Issues',
        'PERICARDIAL EFFUSION': 'Cardiovascular Issues',
        'ALTERED MENTAL STATUS': 'Neurological Issues',
        'ACUTE RESPIRATORY DISTRESS SYNDROME': 'Respiratory Issues',
        'ACUTE RENAL FAILURE': 'Renal Issues',
        'BRADYCARDIA': 'Cardiovascular Issues',
        'CHOLANGITIS': 'Gastrointestinal Issues'
    }
    df_transformed = df_column.map(diagnosis_mapping).fillna('Other')

    return df_transformed