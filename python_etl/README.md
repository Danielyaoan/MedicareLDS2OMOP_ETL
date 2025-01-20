# Python-based ETL of Medicare LDS data to CDMv5-compatible CSV files

This is an implementation of the Medicare LDS Extract-Transform-Load (ETL)
specification designed to generate a set of CDMv5-compatible CSV files
that can then be bulk-loaded into your RDBMS of choice.

The programs have been modified and tested
with 2018 Medicare LDS data and are ready to be used by the general
public.

## Overview of Steps

1) Install required software

1. [Download Medicare LDS input data](#Download-Medicare-LDS-input-data)

1. [Download CDMv5 Vocabulary files](#Download-CDMv5-Vocabulary-Files)

1. [Setup the environment file](#Setup-the-environment-file)

1. [Test ETL with CMS test data](#Test-ETL-with-CMS-test-data)

1. [Run ETL on CMS data](#Run-ETL-on-CMS-data)

1. [Load data into the database](#Load-data-into-the-database)

1. [Open issues and caveats with the ETL](#Open-issues-and-caveats-with-the-ETL)

Further instructions on how to set up the Postgres database can be found [here](postgres_instructions.md).


## Install required software

The ETL process requires Python 3 with the python-dotenv package.

### Linux

Python 3 and python-pip must be installed if they are not already
present. If you are using a RedHat distribution the following commands
will work (you must have system administrator privileges):

``sudo yum install python``  
``sudo yum install python-pip``

The python-pip package enables a non-administrative user to install
python packages for their personal use. From the python_etl directory
run the following command to install the python-dotenv package:

``pip install -r requirements.txt``


## Download Medicare LDS input data
Medicare Limited Data Sets (LDS) are de-identified datasets provided by the Centers for Medicare & Medicaid Services (CMS) for research purposes. These datasets contain detailed healthcare information but exclude direct identifiers to protect privacy. Medicare LDS data is not free and requires researchers to submit a Data Use Agreement (DUA) and a formal request to CMS. Additionally, there are associated fees based on the type and volume of data requested.


### Types of Medicare LDS files: 
For each individual files, make sure to put them under "\Data\medi\linked_ehr" directory with the year as main file name:

- Medicare Beneficiary Data Files:
  
```
\Data\medi\linked_ehr\2018\mbsf_abcd_summary
  
\Data\medi\linked_ehr\2018\mbsf_cc_summary
```

- Medicare Claims Data Files:
  
Inpatient Claims:

```
\Data\medi\linked_ehr\2018\inpatient_base_claims

\Data\medi\linked_ehr\2018\inpatient_revenue_center

\Data\medi\linked_ehr\2018\inpatient_condition_codes

\Data\medi\linked_ehr\2018\inpatient_occurrnce_codes

\Data\medi\linked_ehr\2018\inpatient_span_codes

\Data\medi\linked_ehr\2018\inpatient_value_codes

\Data\medi\linked_ehr\2018\inpatient_demo_codes
```

Outpatient Claims:

```
\Data\medi\linked_ehr\2018\outpatient_base_claims

\Data\medi\linked_ehr\2018\outpatient_revenue_center

\Data\medi\linked_ehr\2018\outpatient_condition_codes

\Data\medi\linked_ehr\2018\outpatient_occurrnce_codes

\Data\medi\linked_ehr\2018\outpatient_span_codes

\Data\medi\linked_ehr\2018\outpatient_value_codes

\Data\medi\linked_ehr\2018\outpatient_demo_codes
```

Carrier (Physician/Supplier Part B) Claims:

```
\Data\medi\linked_ehr\2018\bcarrier_claims

\Data\medi\linked_ehr\2018\bcarrier_line

\Data\medi\linked_ehr\2018\bcarrier_demo_codes
```

Skilled Nursing Facility (SNF) Claims:

```
\Data\medi\linked_ehr\2018\snf_base_claims

\Data\medi\linked_ehr\2018\snf_revenue_center

\Data\medi\linked_ehr\2018\snf_condition_codes

\Data\medi\linked_ehr\2018\snf_occurrnce_codes

\Data\medi\linked_ehr\2018\snf_span_codes

\Data\medi\linked_ehr\2018\snf_value_codes

\Data\medi\linked_ehr\2018\snf_demo_codes
```

Home Health Agency (HHA) Claims:

```
\Data\medi\linked_ehr\2018\hha_base_claims

\Data\medi\linked_ehr\2018\hha_revenue_center

\Data\medi\linked_ehr\2018\hha_condition_codes

\Data\medi\linked_ehr\2018\hha_occurrnce_codes

\Data\medi\linked_ehr\2018\hha_span_codes

\Data\medi\linked_ehr\2018\hha_value_codes

\Data\medi\linked_ehr\2018\hha_demo_codes
```

Hospice Claims:

```
\Data\medi\linked_ehr\2018\hospice_base_claims

\Data\medi\linked_ehr\2018\hospice_revenue_center

\Data\medi\linked_ehr\2018\hospice_condition_codes

\Data\medi\linked_ehr\2018\hospice_occurrnce_codes

\Data\medi\linked_ehr\2018\hospice_span_codes

\Data\medi\linked_ehr\2018\hospice_value_codes

\Data\medi\linked_ehr\2018\hospice_demo_codes
```

Durable Medical Equipment (DME) Claims:

```
\Data\medi\linked_ehr\2018\dme_claims

\Data\medi\linked_ehr\2018\dme_line

\Data\medi\linked_ehr\2018\dme_demo_codes
```

Part D Prescription Drug Event (PDE) File:

```
\Data\medi\linked_ehr\2018\pde_file
```



## Download CDMv5 Vocabulary files
Download vocabulary files from <http://www.ohdsi.org/web/athena/>, ensuring that you select at minimum, the following vocabularies:
SNOMED, ICD9CM, ICD9Proc, CPT4, HCPCS, LOINC, RxNorm, NDC, Gender, Race, CMS Place of Service, ATC, Revenue Code, Ethnicity, NUCC, Medicare Specialty, SPL, Currency, ICD10CM, ABMS, RxNorm Extension, and OMOP Extension

- Unzip the resulting .zip file to a directory.
- Because CPT4 vocabulary is not part of CONCEPT.csv file, one must download it with the provided cpt4.jar program via:
``java -Dumls-user='XXXX' -Dumls-password='XXXX' -jar cpt4.jar 5``, which will append the CPT4 concepts to the CONCEPT.csv file. You will need to pass in your UMLS credentials in order for this command to work. 
- Note: This command works with Java version 10 or below. 

## Setup the environment file
Edit the variables in the .env file which specify various directories used during the ETL process.

- Path to the directory that will hold the control files. This contains files used for auto-incrementing record numbers and keeping track of physicians and physician institutions. These
files need to be deleted if you want to restart numbering.

BASE_ETL_CONTROL_DIRECTORY=\Data\control

- Path to the directory containing the linked medicare LDS files. Be sure to create an extra directory for each year of data you will be using.
 
BASE_MEDI_INPUT_DIRECTORY=\Data\medi\linked_ehr

- Path to the directory containing the OMOP Vocabulary v5 files (can be downloaded from http://www.ohdsi.org/web/athena/).

BASE_OMOP_INPUT_DIRECTORY=\Data\cdmv5

- Path to the directory where CDM-compatible CSV files should be saved. This might include duplicated information.

BASE_OUTPUT_DIRECTORY=\Data\output

- Path to the directory where updated non-duplicated outcomes should be saved.
 
BASE_UPDATE_OUTPUT_DIRECTORY=\Data\updated_output

## Test ETL with test data
You could create a subset of data of each Medicare LDS files for test run.

Run the ETL process on the files in this directory(if your data is 2018) with:

``python LDS_ETL_CDM_v5.py 2018``  

and check for the output generated in the BASE_OUTPUT_DIRECTORY and BASE_UPDATE_OUTPUT_DIRECTORY
directory.  A .csv file should be generated for each of the CDM v5 tables,
suitable for comparing against the hand-coded outputs.  Note at this
time, all of the tables have been implemented, but some might be empty (e.g visit_cost and device_cost) due to lack of data.
Clean out the control files in BASE_ETL_CONTROL_DIRECTORY before running the next step.

## Run ETL on Medicare LDS data

To process, run:

- ``python LDS_ETL_CDM_v5.py <year number>``
    - Where ``<year number>`` is the year you are using from Medicare LDS
    - e.g. ``python LDS_ETL_CDM_v5.py 2018`` will run the ETL on the Medicare LDS data in the 2018 directory
    - The resulting output files should be suitable for bulk loading into a CDM v5 database.

The runs cannot be done in parallel because counters and unique
physician and physician institution providers are detected and carried
over multiple runs (saved in BASE_ETL_CONTROL_DIRECTORY). We
recommend running them sequentially for each year to produce a
complete ETL.

## Load data into the database
The PostgreSQL database was used for data quality assessment with [Data Quality Dashboard](https://github.com/OHDSI/DataQualityDashboard)

## Open issues and caveats with the ETL
a) The concepts for Unknown Race, Non-white, and Other Race (8552, 9178, and 8522) have been deprecated, so race_concept_id in Person file has been populated with
    '0' for these deprecated concepts.

b) Only two ethnicity concepts (38003563, 38003564) are available.  38003563: Hispanic and  38003564: Non-Hispanic.

c) When a concept id has no mapping in the CONCEPT_RELATIONSHIP table:
- If there is no mapping from OMOP (ICD9) to OMOP (SNOMED) for an ICD9 concept id, target_concept_id for such ICD9 concept id is populated with '0' .
- If there is no self-mapping from OMOP (HCPCS/CPT4) to OMOP (HCPCS/CPT4) for an HCPCS/CPT4 concept id, target_concept_id for such HCPCS/CPT4 concept id is populated with '0' .
- If there is no mapping from OMOP (NDC) to OMOP (RxNorm) for an NDC concept id, target_concept_id for such NDC concept id is populated with '0'.

d) The source data contains concepts that appear in the CONCEPT.csv file but do not have relationship mappings to target vocabularies. For these, we create records with concept_id 0 and include the source_concept_id in the record.

e) The current steps only include variables those were mapped in [CMS-Synpuf-ETL-CDM](https://github.com/OHDSI/ETL-CMS/tree/master), some of the records are currently not mapped to cdm table yet, such as device_cost, drug_exposure and visit_cost. That's why some columns seem to have value in Medicare LDS but not shown in output result.

f) Input files might have some ICD9/HCPCS/NDC codes that are
not defined in the concept file and therefore such records are not
processed by the program and are written to the
'unmapped_code_log.txt' file. This file is opened in append mode so
that if more than one input file is processed together, the program
should append unmapped codes from all input files instead of
overwriting. So the file 'unmapped_code_log.txt' must be deleted if
you want to rerun the program with the same input file.
