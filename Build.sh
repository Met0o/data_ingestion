@echo off

REM Update pip
python -m pip install --upgrade pip

REM Install dependencies
pip install -r requirements.txt

#REM Run tests
#pytest