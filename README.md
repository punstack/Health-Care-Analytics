# Analysis of the MIMIC-III Dataset

This study leverages the MIMIC-III (Medical Information Mart for Intensive Care III) database, which contains health data from ICU patients at the Beth Israel Deaconess Medical Center (2001-2012). This study investigates patient demographics, admission trends, and developed predictive models for ICU readmission. The analysis highlights that older adults (ages 58 and above) dominate ICU admissions, with gender and ethnicity showing minimal impact on ICU stay length. Cardiovascular (19.0%) and respiratory (15.4%) issues were the most common reasons for admission across patients in the dataset.

Seasonal and time-of-day patterns were observed in ICU mortality, with higher death rates in winter and during evening hours. Additionally, several models were built to predict patient readmission within 30 days. The best performing model was logistic regression with oversampling, achieving an accuracy of 94% and an F1 score of 0.948. These findings emphasize the utility of machine learning in identifying high-risk patients and improving critical care outcomes.

## Introduction

MIMIC-III (Medical Information Mart for Intensive Care III) is a large, publicly available database composed of detailed health information related to deidentified patients admitted to critical care units of the Beth Israel Deaconess Medical Center between 2001 and 2012 ([Johnson et al., 2016](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4878278/)). This database has been utilized by researchers across the globe to study patient outcomes in critical care, facilitate the development of predictive models, and support the application of machine learning techniques to analyze patient health data.

This relational database consists of 26 tables, encompassing a broad range of clinical data including detailed information such as patient demographics, vital signs, laboratory results, and diagnoses ([Johnson et al., 2016](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4878278/)). Its detail and scope have made it a valuable resource of medical research, enabling studies that span fields such as artificial intelligence, personalized medicine, and computer science for the prediction of health outcomes ([Rajkomar et al., 2018](https://www.nature.com/articles/s41746-018-0029-1); [Che et al., 2018](https://www.nature.com/articles/s41598-018-24271-9); [McManamay & DeRolph, 2019](https://www.nature.com/articles/sdata201917)).

This study utilizes MIMIC-III version 1.4, which is the most current version of the database at the time of analysis ([Johnson, 2016](https://doi.org/10.13026/C2XW26)). The analysis conducted in this journal explores the distribution of patient demographics, time series analysis of ICU admission, and predicting the likelihood of patient readmission. Through the use of detailed graphics and machine learning algorithms, this study aims to provide insight into patterns in critical care admission and improve patient outcomes.

## Related Figures
<p align="center">
  <img src ="https://github.com/user-attachments/assets/167dcd6c-cd41-4e83-9f5f-af330c3093aa" />
</p>

Patients faced one of three outcomes: "death within hospital," "death within 90 days of discharge," or "death not within 90 days of discharge." This histogram displays the patient mortality rate within the hospital. It is noteworthy that the majority of patients did not die within the hospital care, despite elderly adults (>60 years of age) being the majority of patients admitted to the ICU.

<p align="center">
  <img src = "https://github.com/user-attachments/assets/44ce069f-f22f-445e-8124-6c7aa2b02dcb"/>
</p>

This diagram depicts the most common reasons for ICU admittance. Cardiovascular, repsiratory, and gastrointestinal issues make up the three most common diagnoses for ICU patients. See `diagnosis.py` for a more detail on how diagnoses were categorized.

<p align="center">
  <img src ="https://github.com/user-attachments/assets/ffef2fcc-96ea-44bf-a480-b72ac5699a29"/>
</p>

Diagnosis categories were split between age groups of ICU patients and normalized to 100% for ease of viewing. Based on the percentages, it is relatively easy to see which diagnosis categories dominate in which age group.

<p align="center">
  <img src ="https://github.com/user-attachments/assets/723cc9b9-22ac-4dcd-9b74-81b5d6c78de7" />
</p>

Time of death was divided into months and then grouped into seasons. While there were no statistically significant differences by season, the lowest hospital expiry rate occured in the summer and the highest in the winter.

<p align="center">
  <img src = "https://github.com/user-attachments/assets/2950d0b2-af5e-44ca-aa68-ee2d172de906" />
</p>
  
Time of death was divded by the hour and grouped into time of day. Interestingly, mornings (6AM to 12PM) experienced the lowest hospital expiry rate at 6% compared to the other times of day.

Model performance before and after oversampling/undersampling is summarized below:
| Model | F1 Score (Raw Data) | F1 Score (Oversampling + Undersampling) | Accuracy (Raw Data) | Accuracy (Oversampling + Undersampling) |
| --- | --- | --- | --- | --- |
| Logistic Regression | 0.157 | 0.948 | 54% | 94% |
| Decision Tree | 0.153 | 0.770 | 44% | 68% |
| Random Forest | 0.157 | 0.893 | 48% | 89% |

###### Copyright (c) 2019 MIT Laboratory for Computational Physiology
