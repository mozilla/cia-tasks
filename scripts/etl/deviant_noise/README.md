# Deviant Noise ETL

Schedule jobs that pull Perfherder performance data, and produce devaint noise aggregates for every series

## Contributing to Development

### Install 

Install into virtual machine using the directory of this README as current directory

    c:\python37\python.exe -m pip install virtualenv
    c:\python37\python.exe -m virtualenv .venv             
    .venv\Scripts\activate
    
We use `pip-compile` to generate a fixed requirements.txt file  
    
    pip install pip-tools
    pip-compile --upgrade --generate-hashes --output-file requirements.txt requirements.in
    pip install -r requirements.txt

then you may run the schedulers ETL

    export PYTHONPATH=.:vendor
    python3 main.py --config=config-local.json
    

### Execution

#### Scope for S3 cache

auth:aws-s3:read-write:communitytc-bugbug/data/adr_cache/*