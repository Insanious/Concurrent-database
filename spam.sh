#!/bin/bash


make -j -s clean_client && make client

grep 'CREATE TABLE' args.txt | parallel -I% --max-args 1 ./client %
