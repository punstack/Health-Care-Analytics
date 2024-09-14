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
        r'INJURY': 'Trauma/Injury',
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
        r'GASTROINTESTINAL': 'Gastrointestinal Issues',
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
        r'PREMATURITY': 'Neonatal Care',
        r'GASTROPARESIS': 'Gastrointestinal Issues',
        r'TACHYCARDIA': 'Cardiovascular Issues',
        r'HEPATIC': 'Liver Issues',
        r'SUBDURAL': 'Neurological Issues',
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
        r'CHOLANGIOPANCREATOGRAPHY': 'Gastrointestinal Issues',
        r'HEMOPTYSIS': 'Respiratory Issues',
        r'HYPERGLYCEMIA': 'Blood Conditions',
        r'HEART BLOCK': 'Cardiovascular Issues',
        r'CHRONIC OBST PULM DISEASE': 'Respiratory Issues',
        r'AIRWAY': 'Respiratory Issues',
        r'\+ETT.?\\?CATH': 'Respiratory Issues',
        r'\b\w*ANEURYSM\w*\b': 'Neurological Issues',
        r'LYMPHOMA': 'Cancer',
        r'ATRIAL FIBRILLATION': 'Cardiovascular Issues',
        r'COLITIS': 'Gastrointestinal Issues',
        r'CHF': 'Cardiovascular Issues',
        r'TRACHEAL STENOSIS': 'Respiratory Issues',
        r'DIVERTICULITIS': 'Gastrointestinal Issues',
        r'CIRRHOSIS': 'Liver Issues',
        r'LEUKEMIA': 'Cancer',
        r'STATUS EPILEPTICUS': 'Neurological Issues',
        r'DKA': 'Blood Conditions',
        r'SEPTIC': 'Infections',
        r'INCISIONAL HERNIA/SDA': 'Gastrointestinal Issues',
        r'PEDESTRIAN STRUCK': 'Trauma/Injury',
        r'VENTRAL HERNIA/SDA': 'Gastrointestinal Issues',
        r'SPLENIC': 'Gastrointestinal Issues',
        r'BRIGHT RED BLOOD PER RECTUM': 'Gastrointestinal Issues',
        r'SPINAL': 'Neurological Issues',
        r'INFECTED': 'Infections',
        r'SAH': 'Neurological Issues',
        r'PNEUMOTHORAX': 'Respiratory Issues',
        r'MENINGITIS': 'Infections',
        r'SCOLIOSIS': 'Neurological Issues',
        r'CARDIOGENIC': 'Cardiovascular Issues',
        r'HTN': 'Blood Conditions',
        r'HEMATOMA': 'Trauma/Injury',
        r'LUMAR STENOSIS/SDA': 'Neurological Issues',
        r'HEMOTHORAX': 'Trauma/Injury',
        r'ABSCESS': 'Infections',
        r'ABD PAIN': 'Gastrointestinal Issues',
        r'ABD. PAIN': 'Gastrointestinal Issues',
        r'GUN SHOT WOUND': 'Trauma/Injury',
        r'S/P ARREST': 'Cardiovascular Issues',
        r'DEEP VEIN THROMBOSIS': 'Blood Conditions',
        r'ICH': 'Neurological Issues',
        r'THROMBOCYTOPENIA': 'Blood Conditions',
        r'APPENDICITIS': 'Gastrointestinal Issues',
        r'NON Q MI\\CATH': 'Cardiovascular Issues',
        r'INCARCERATED HERNIA': 'Gastrointestinal Issues',
        r'\bMI\b': 'Cardiovascular Issues',
        r'STRIDOR': 'Respiratory Issues',
        r'MESENTERIC ISCHEMIA': 'Gastrointestinal Issues',
        r'TRACHEOBRONCHOMALACIA': 'Respiratory Issues',
        r'PERITONITIS': 'Infections',
        r'DELERIUM': 'Neurological Issues',
        r'CAD': 'Cardiovascular Issues',
        r'PERIPHERAL VASCULAR DISEASE': 'Blood Conditions',
        r'ISCHEMIC BOWEL': 'Blood Conditions',
        r'HYDROCEPHALUS': 'Neurological Issues',
        r'MYASTHENIA GRAVIS': 'Neurological Issues',
        r'CVA': 'Neurological Issues',
        r'ISCHEMIC': 'Blood Conditions',
        r'MITRAL STENOSIS': 'Cardiovascular Issues',
        r'INTERCRANIAL HEMORRHAGE': 'Neurological Issues',
        r'GANGRENE': 'Blood Conditions',
        r'FEBRILE NEUTROPENIA': 'Blood Conditions',
        r'MR\\MITRAL VALVE REPLACEMENT MINIMALLY INVASIVE APPROACH/SDA': 'Cardiovascular Issues',
        r'AICD FIRING': 'Cardiovascular Issues',
        r'UGIB': 'Gastrointestinal Issues',
        r'PNEMONIA': 'Respiratory Issues',
        r'KETO ACIDOSIS': 'Blood Conditions',
        r'TAMPONADE': 'Cardiovascular Issues',
        r'ASSAULT': 'Trauma/Injury',
        r'PANCYTOPENIA': 'Blood Conditions',
        r'CARDIOMYOPATHY': 'Cardiovascular Issues',
        r'SOB': 'Respiratory Issues',
        r'JAUNDICE': 'Liver Issues',
        r'BOWEL': 'Gastrointestinal Issues',
        r'VTACH': 'Cardiovascular Issues',
        r'VOLUME OVERLOAD': 'Blood Conditions',
        r'EMPYEMA': 'Respiratory Issues',
        r'KIDNEY': 'Kidney Issues',
        r'STAB WOUND': 'Trauma/Injury',
        r'RAPID A-FIB': 'Cardiovascular Issues',
        r'PANCREATIC PSEUDOCYST': 'Gastrointestinal Issues',
        r'TRACHEAL': 'Respiratory Issues',
        r'BRONCHIAL': 'Respiratory Issues',
        r'TRACHEOBRONCOMALICIA': 'Respiratory Issues',
        r'MITRAL VSLVE': 'Cardiovascular Issues',
        r'L1 DISC PROTRUSION': 'Neurological Issues',
        r'METASTATIC': 'Cancer',
        r'MELANOMA': 'Cancer',
        r'AFIB/PNA': 'Cardiovascular Issues',
        r'AFIB': 'Cardiovascular Issues',
        r'HEMIPARESIS': 'Neurological Issues',
        r'PERIPHERAL VASCULAR': 'Blood Conditions',
        r'WOUND': 'Trauma/Injury',
        r'MALIGNANT NEOPLASM': 'Cancer',
        r'VISUAL': 'Neurological Issues',
        r'CHOALNGITIS': 'Gastrointestinal Issues',
        r'POLYP ADENOMATOUS': 'Gastrointestinal Issues',
        r'GASTRO INTESTINAL': 'Gastrointestinal Issues',
        r'DYSPNIA': 'Respiratory Issues',
        r'RECTAL CA/GIB': 'Cancer',
        r'APPENDECTOMY': 'Gastrointestinal Issues',
        r'CHIARI': 'Neurological Issues',
        r'MITRAL VASLVE': 'Cardiovascular Issues',
        r'METRAL': 'Cardiovascular Issues',
        r'ESOPHAGEAL': 'Gastrointestinal Issues',
        r'CHOLEDOCALITHIASIS': 'Gastrointestinal Issues',
        r'AMPULLARY': 'Gastrointestinal Issues',
        r'NUERO': 'Neurological Issues',
        r'PARENCHYMAL': 'Neurological Issues',
        r'TAMPONEDE': 'Cardiovascular Issues',
        r'LACERATION': 'Trauma/Injury',
        r'GLIOBLASTOMA': 'Cancer',
        r'FRACTURES': 'Trauma/Injury',
        r'GULLIAN BARRE': 'Neurological Issues',
        r'NASOPHARNGEAL CA': 'Cancer',
        r'FEMUR NON UNION': 'Trauma/Injury',
        r'ASD REPAIR': 'Cardiovascular Issues',
        r'HYPERBILIRUBERIMIA': 'Blood Conditions',
        r'TB': 'Respiratory Issues',
        r'V-FIB': 'Cardiovascular Issues',
        r'ISCHEMIA': 'Blood Conditions',
        r'THROMBOEMBOLISM': 'Blood Conditions',
        r'ARTHROSLOROSIS': 'Blood Conditions',
        r'ANTERIOR CORD SYNDROME': 'Neurological Issues',
        r'ATRIAL': 'Cardiovascular Issues',
        r'MPOTOR VEHICLE ACCIDENT': 'Trauma/Injury',
        r'PERIPHERAL INSUFFICIENCY': 'Blood Conditions',
        r'LYNPHOMA': 'Cancer',
        r'MENIGIOMA': 'Cancer',
        r'HYPERBILIRUBEN': 'Liver Issues',
        r'PNAUMONIA': 'Respiratory Issues',
        r'\bCARCINOMA\b': 'Cancer',
        r'PVD': 'Blood Conditions',
        r'NECROTIZING FASCITIS': 'Infections',
        r'MELENA': 'Gastrointestinal Issues',
        r'BRAIN CA': 'Cancer',
        r'METHYLGLOBLUIN': 'Blood Conditions',
        r'MR\\MITRAL': 'Cardiovascular Issues',
        r'CABG\\STERNAL WIRE REMOVAL': 'Cardiovascular Issues',
        r'HEMOPARINEUM': 'Neurological Issues',
        r'STRUCK BY CAR': 'Trauma/Injury',
        r'LOBE LESION': 'Neurological Issues',
        r'DYPSNEA': 'Respiratory Issues',
        r'SVC SYNDROME': 'Cardiovascular Issues',
        r'RECANALIZATION': 'Cardiovascular Issues',
        r'ENCEPHALOMYELITIS': 'Neurological Issues',
        r'INTRAPRAECHYMAL': 'Neurological Issues',
        r'CROHNS': 'Gastrointestinal Issues',
        r'\bGI\b': 'Gastrointesitnal Issues',
        r'EPELEPTICUS': 'Neurological Issues',
        r'IRREGULAR HEART': 'Cardiovascular Issues',
        r'\bPVD\b': 'Blood Conditions',
        r'PACEMAKER': 'Cardiovascular Issues',
        r'DISSECTING': 'Cardiovascular Issues',
        r'MIDLINE HERNIA': 'Gastrointestinal Issues',
        r'INCONTINENCE': 'Gastrointestinal Issues',
        r'PELVIC': 'Gastrointestinal Issues',
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