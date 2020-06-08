# Deviant Noise ETL

Schedule jobs that pull Perfherder performance data, and produce devaint noise aggregates for every series

## Coding

### Install 

Install into virtual machine using the directory of this README as current directory

    python -m pip install virtualenv
    python -m virtualenv .venv             
    .venv\Scripts\activate
    
the moxci has some conflicting requirements, they can be sorted with `pip-tools`    
    
    pip3 install pip-tools
    pip-compile --upgrade --generate-hashes --output-file requirements.txt requirements.in
    pip3 install -r requirements.txt

then you may run the schedulers ETL

    export PYTHONPATH=.:vendor
    python3 main.py --config=config-local.json
    

### Execution

#### Scope for S3 cache

auth:aws-s3:read-write:communitytc-bugbug/data/adr_cache/*