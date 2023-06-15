#!/bin/bash

# If you need to add or remove from the repo root please change NUM_ITEMS_SHOULD_BE

# Please take care to only add files or directories to the repo root unless they are
# required to be in the repo root, otherwise please find a more appropriate location.

NUM_ITEMS_SHOULD_BE=40
NUM_ITEMS=$(ls -la | wc -l)

echo "Items found in repo root:"
ls -la

if [ $NUM_ITEMS -ne $NUM_ITEMS_SHOULD_BE ];
then
  echo "There should be ${NUM_ITEMS_SHOULD_BE} items in the repo root, you have ${NUM_ITEMS}"
  exit 1
fi
