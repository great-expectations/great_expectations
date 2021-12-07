python_pip = """
python -m pip install great_expectations
python -m ensurepip --upgrade
"""

python3_pip = """
python3 -m pip install great_expectations
python3 -m ensurepip --upgrade
"""

python_conda = """
conda --version
python -m conda install -c conda-forge great-expectations
"""

python3_conda = """
conda --version
python3 -m conda install -c conda-forge great-expectations
"""