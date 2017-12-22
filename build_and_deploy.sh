#!/usr/bin/env bash
rm -r dist/*
rm -r build/*
rmdir dist
rmdir build

python setup.py sdist
python setup.py bdist_wheel

twine upload dist/*
