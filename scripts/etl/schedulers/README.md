# Schedulers ETL

Schedule some jobs to run and fill BigQuery


## Coding

### Install 

Install into virtual machine using the directory of this README as current directory

    python -m pip install virtualenv
    python -m virtualenv .venv             
    .venv\Scripts\activate
    pip install -r requirements.txt

### Execution

#### Scope for S3 cache

auth:aws-s3:read-write:communitytc-bugbug/data/adr_cache/*