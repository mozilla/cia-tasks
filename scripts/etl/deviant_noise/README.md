# Deviant Noise ETL

Schedule jobs that pull Perfherder performance data, and produce devaint noise aggregates for every series

https://docs.google.com/document/d/14_i11KVVvd8keyUROAW0xgRTldoWCx0OZS8kXUuSH2o/edit

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

