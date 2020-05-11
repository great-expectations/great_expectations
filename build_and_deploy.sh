#!/usr/bin/env bash
rm -r dist/*
rm -r build/*
rm -r great_expectations.egg-info/*
rmdir dist
rmdir build
rmdir great_expectations.egg-info

python setup.py sdist
python setup.py bdist_wheel

twine upload dist/*

rm -r dist/*
rm -r build/*
rm -r great_expectations.egg-info/*
rmdir dist
rmdir build
rmdir great_expectations.egg-info
