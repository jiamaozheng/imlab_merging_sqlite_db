# Imlab-merging-multiple-sqlite-db
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
