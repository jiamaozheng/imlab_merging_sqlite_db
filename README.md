# merge-multiple-sqlite-db
These scripts are used to merge multiple tissue predictdb sqlite db into one db


## Installation
```bash 
https://github.com/jiamaozheng/imlab_merging_sqlite_db.git
``` 

## Run  
**Usage 1**
 ```bash 
python merging_sqlites_v6_new.py -m GTEx-V6p_New_HapMap-2017-11-29.sqlite -i v6p_new/ -o v6p_new/ -l v6p_new/

 ``` 

**Usage 2**
 ```bash 
python merging_sqlites_v6p_old.py -m DGN-HapMap-2015.sqlite -i DGN-HapMap-2015 -o DGN-HapMap-2015 -l DGN-HapMap-2015
python merging_sqlites_v6p_old.py -m GTEx-V6p-1KG-2016-11-16.sqlite -i GTEx-V6p-1KG-2016-11-16 -o GTEx-V6p-1KG-2016-11-16 -l GTEx-V6p-1KG-2016-11-16
python merging_sqlites_v6p_old.py -m GTEx-V6p-HapMap-2016-09-08.sqlite -l GTEx-V6p-HapMap-2016-09-08 -i GTEx-V6p-HapMap-2016-09-08 -o GTEx-V6p-HapMap-2016-09-08
 ``` 

 **Usage 3**
 ```bash 
python merging_sqlites_v7.py -m GTEx-V7-HapMap-2017-11-29.sqlite -i v7/ -o v7/ -l v7/
 ``` 

 **Usage 4**
 ```bash 
python merging_sqlites_allv7.py -m GTEx-V7-HapMap-2017-11-29.sqlite -i v7/ -o v7/ -l v7/
 ``` 

## Export data to Google Cloud BigQuery
```bash
bq --nosync load --field_delimiter=',' --autodetect test.extra /Users/jiamaozheng/Dropbox/Im_lab/2017/predictdb/sources/DGN-HapMap-2015/DGN-HapMap-2015_extra_final.csv
```


## Export data to AWS RDS
 Exporting data from the SQLite database to a CSV file using sqitebrowser. 
---
  * Please download <a href='http://sqlitebrowser.org'> sqiteborwser </a>. 

  * Please reformat header by following table column name and remove all NAs for each CSV file. For example, column name something like `pred.perf.pval` should be reformatted to `pred_perf_pval`. PostgreSQL database won't accept `pred.perf.pval` and `NA`. 

Copying all data to the EC2 instances (Please skip this step if you run your jobs from your local Postgre). 
---
``` 
 scp /YOUR_PATH/DGN-HapMap-2015/*.csv ubuntu@ec2-52-207-78-75.compute-1.amazonaws.com:/home/ubuntu/dng 
 scp /YOUR_PATH/GTEx-V6p-1KG-2016-08-18/*.csv ubuntu@ec2-52-207-78-75.compute-1.amazonaws.com:/home/ubuntu/1kg 
 scp /YOUR_PATH/GTEx-V6p-1KG-2016-11-16/*.csv ubuntu@ec2-52-207-78-75.compute-1.amazonaws.com:/home/ubuntu/1kg_new 
 scp /YOUR_PATH/GTEx-V6p-HapMap-2016-09-08/*.csv ubuntu@ec2-52-207-78-75.compute-1.amazonaws.com:/home/ubuntu/hapmap 
```


Creating a PostgreSQL Database Instance
---
<a href='https://aws.amazon.com/getting-started/tutorials/create-connect-postgresql-db/'> Create and Connect to a PostgreSQL Database
with Amazon RDS </a> 

Connecting to a PostgreSQL Database Instance.
---
```
psql \
  --host=predictdb.ccsriudvs1y0.us-east-1.rds.amazonaws.com \
  --port=5432 \
  --username predictdb \
  --password \
  --dbname=dgn_hapmap_2015


psql \
  --host=predictdb.ccsriudvs1y0.us-east-1.rds.amazonaws.com \
  --port=5432 \
  --username predictdb \
  --password \
  --dbname=gtex_v6p_1kg_2016_08_18

psql \
  --host=predictdb.ccsriudvs1y0.us-east-1.rds.amazonaws.com \
  --port=5432 \
  --username predictdb \
  --password \
  --dbname=gtex_v6p_1kg_2016_11_16

psql \
  --host=predictdb.ccsriudvs1y0.us-east-1.rds.amazonaws.com \
  --port=5432 \
  --username predictdb \
  --password \
  --dbname=gtex_v6p_hapmap_2016_09_08
```



Listing all databases 
---
```
\list or \l
```


Listing all tables in the current database
---
```
\dt 
```



Creating databases
---
```
DROP DATABASE IF EXISTS dgn_hapmap_2015;
CREATE DATABASE dgn_hapmap_2015;

DROP DATABASE IF EXISTS gtex_v6p_1kg_2016_08_18;
CREATE DATABASE gtex_v6p_1kg_2016_08_18;

DROP DATABASE IF EXISTS gtex_v6p_1kg_2016_11_16;
CREATE DATABASE gtex_v6p_1kg_2016_11_16;

DROP DATABASE IF EXISTS gtex_v6p_hapmap_2016_09_08;
CREATE DATABASE gtex_v6p_hapmap_2016_09_08;
```

Connecting databases
---
```
\connect dgn_hapmap_2015
\connect gtex_v6p_hapmap_2016_09_08
\connect gtex_v6p_1kg_2016_08_18
\connect gtex_v6p_1kg_2016_11_16

```


Creating tables for each database. 
---
```
DROP TABLE IF EXISTS weights; 
CREATE TABLE weights (
    rsid TEXT,
    gene TEXT, 
    weight REAL, 
    ref_allele TEXT, 
    eff_allele TEXT, 
    tissue TEXT, 
    PRIMARY KEY (tissue, rsid, gene)
    );

DROP TABLE IF EXISTS extra; 
CREATE TABLE extra (
    gene TEXT, 
    genename TEXT, 
    pred_perf_R2 double precision, 
    n_snps_in_model INTEGER, 
    pred_perf_pval double precision, 
    pred_perf_qval double precision, 
    tissue TEXT, 
    PRIMARY KEY (tissue, gene)
    ); 

DROP TABLE IF EXISTS construction; 
CREATE TABLE construction (
    chr INTEGER, 
    cv_seed INTEGER, 
    tissue TEXT, 
    PRIMARY KEY (tissue, chr)
    ); 

DROP TABLE IF EXISTS sample_info; 
CREATE TABLE sample_info (
    n_samples INTEGER, 
    tissue TEXT, 
    PRIMARY KEY (tissue)
    ); 
```

Using the `\copy` command to import data to a table on a PostgreSQL DB instance
---
```
\copy construction from 'construction.csv' with delimiter ',' CSV HEADER; 
\copy sample_info from 'sample_info.csv' with delimiter ',' CSV HEADER; 
\copy extra from 'extra.csv' with delimiter ',' CSV HEADER; 
\copy weights from 'weights.csv' with delimiter ',' CSV HEADER; 
```
