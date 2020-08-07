#!/bin/bash


git clone --no-single-branch --depth 50 https://github.com/great-expectations/great_expectations.git .
git fetch origin --force --tags --prune --prune-tags --depth 50 pull/1712/head:external-1712
git checkout --force 590ecfc4a0c63ff7e9e64215cd197f9f93808f0a
git clean -d -f -f
python3.8 -mvirtualenv --system-site-packages /home/docs/checkouts/readthedocs.org/user_builds/great-expectations-great-expectations/envs/1712
/home/docs/checkouts/readthedocs.org/user_builds/great-expectations-great-expectations/envs/1712/bin/python -m pip install --upgrade --no-cache-dir pip
/home/docs/checkouts/readthedocs.org/user_builds/great-expectations-great-expectations/envs/1712/bin/python -m pip install --upgrade --no-cache-dir -I Pygments==2.3.1 setuptools==41.0.1 docutils==0.14 mock==1.0.1 pillow==5.4.1 "alabaster>=0.7,<0.8,!=0.7.5" commonmark==0.8.1 recommonmark==0.5.0 sphinx<2 sphinx-rtd-theme<0.5 readthedocs-sphinx-ext<1.1
/home/docs/checkouts/readthedocs.org/user_builds/great-expectations-great-expectations/envs/1712/bin/python -m pip install --exists-action=w --no-cache-dir -r requirements.txt
/home/docs/checkouts/readthedocs.org/user_builds/great-expectations-great-expectations/envs/1712/bin/python -m pip install --exists-action=w --no-cache-dir -r docs/requirements.txt

cp docs/conf.py docs/conf.py.bak
cat ../rtd_conf_supplement.py >> docs/conf.py
cat docs/conf.py

/home/docs/checkouts/readthedocs.org/user_builds/great-expectations-great-expectations/envs/1712/bin/python -m pip install --exists-action=w --no-cache-dir -r docs/requirements.txt

## THESE STEPS ARE NOT REFLECTED IN THE RTD BUILD
source /home/docs/checkouts/readthedocs.org/user_builds/great-expectations-great-expectations/envs/1712/bin/activate
cd docs/
##

python /home/docs/checkouts/readthedocs.org/user_builds/great-expectations-great-expectations/envs/1712/bin/sphinx-build -T -E -b readthedocs -d _build/doctrees-readthedocs -D language=en . _build/html
