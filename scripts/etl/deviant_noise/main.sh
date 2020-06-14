cd /cia-tasks/scripts/etl/deviant_noise
pip3 --version
pip3 install pip-tools
pip-compile --upgrade --generate-hashes --output-file requirements.txt requirements.in
pip3 install -r requirements.txt
python3 --version
export PYTHONPATH=.:vendor
python3 main.py
