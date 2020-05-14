apt update
apt install -y python3 python3-pip python3-venv
ln -s /usr/bin/pip3 /usr/bin/pip
curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python
source $HOME/.poetry/env
poetry install
poetry run ./bin/init.py
