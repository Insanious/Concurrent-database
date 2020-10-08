#!/bin/bash


make -j -s clean_client && make -j -s client

# grep 'CREATE TABLE' args.txt | parallel -I% --max-args 1 ./client %

# for i in {1..64}; do
# 	# ./client "CREATE TABLE students (id INT, first_name VARCHAR(7), last_name VARCHAR(8));"
# 	./client "CREATE TABLE students (id INT, first_name VARCHAR(7), last_name VARCHAR(8));"

# done
./client "CREATE TABLE students (id INT, first_name VARCHAR(7), last_name VARCHAR(8));"
./client "INSERT INTO students VALUES (42, 'Oscar', 'Svensson');"
