#!/bin/bash

docker-compose run -T linkchecker https://docs.greatexpectations.io/en/latest/ -o csv/utf_8 2> /dev/null > ge_docs_links.csv
