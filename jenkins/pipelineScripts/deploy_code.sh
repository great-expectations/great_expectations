virtualenv --python=python3 distenv
source distenv/bin/activate
pip install --upgrade pip
pip install twine
pip install wheel
python setup.py sdist
python setup.py bdist_wheel
python -m twine upload --repository-url https://msnexus.morningstar.com/repository/pypi-hosted/ dist/*
