# Schedulers ETL

Schedule some jobs to run and fill BigQuery


## Coding

Work is proceeding on the `etl-schedulers` branch

    git checkout etl-schedulers



### Running

Install into virtual machine using the directory of this README as current directory

    python -m pip install virtualenv
    python -m virtualenv .venv             
    .venv\Scripts\activate
    
ensure you are on the correct branch

    git checkout etl-schedulers

the moxci has some conflicting requirements, they can be sorted with `pip-tools`    
    
    pip3 install pip-tools
    pip-compile --upgrade --generate-hashes --output-file requirements.txt requirements.in
    pip3 install -r requirements.txt

then you may run the schedulers ETL

    export PYTHONPATH=.:vendor
    python3 main.py --config=config-local.json

