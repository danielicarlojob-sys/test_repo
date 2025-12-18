set up virtual enviroment:  
    1 - navigate to working dir:                                            cd path\to\project
    2 - create venv (sencond venv is the name of the virtual enviroment):   python -m venv venv
    3 - activate venv:                                                      venv\Scripts\activate
    4 - deactivate venv:                                                    deactivate
    5 - install all modules requirements                                    pip install -r requirements.txt

run tests:  PYTHONPATH=%cd% && pytest --testdox
--- test mod for git hub ---
