#!/bin/bash

# make -s -j re
make -s -j client

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:lib
SLEEP=0.01
# ./db &
sleep $SLEEP

rm -f ../database/*.txt

echo -e "\nCREATE TABLE that doesn't exists:"
./client "CREATE TABLE students (id INT, first_name VARCHAR(7), last_name VARCHAR(8));"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e "CREATE TABLE that already exists:"
./client "CREATE TABLE students (id INT, first_name VARCHAR(7), last_name VARCHAR(8));"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e ".tables:"
./client ".tables"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e ".schema students:"
./client ".schema students"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e "INSERT INTO table that doesn't exist:"
./client "INSERT INTO teachers VALUES (42, 'Oscar', 'Svensson');"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e "INSERT INTO table that exist:"
./client "INSERT INTO students VALUES (42, 'Oscar', 'Svensson');"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e "SELECT FROM table that doesn't exist:"
./client "SELECT * FROM teachers;"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e "SELECT FROM table that exist:"
./client "SELECT * FROM students;"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e "DROP TABLE that exists:"
./client "DROP TABLE students;"
echo -e "\n-------------------\n"
sleep $SLEEP

echo -e "DROP TABLE that doesn't exist:"
./client "DROP TABLE students;"
echo -e "\n-------------------\n"
sleep $SLEEP

# killall db
# ./client "SELECT * FROM students;"
# ./client "CREATE TABLE students (id INT, first_name VARCHAR(7), last_name VARCHAR(8), PRIMARY KEY(id));"
# ./client "SELECT first_name, last_name FROM students;"
# ./client "DELETE FROM students WHERE id=42;"
# ./client "UPDATE students SET first_name='Emil', last_name='Johansson' WHERE id=42;"
