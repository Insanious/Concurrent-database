#!/bin/sh

make -s -j re
make -s -j client

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:lib

./db &
sleep 1

./client "CREATE TABLE students (id INT, first_name VARCHAR(7), last_name VARCHAR(8));" &
# ./client ".tables" &
# ./client ".schema students" &
# ./client "DROP TABLE students;"
# ./client "INSERT INTO students VALUES (42, 'Oscar', 'Svensson');"
# ./client "SELECT * FROM students;"
# ./client "CREATE TABLE students (id INT, first_name VARCHAR(7), last_name VARCHAR(8), PRIMARY KEY(id));"
# ./client "SELECT first_name, last_name FROM students;"
# ./client "DELETE FROM students WHERE id=42;"
# ./client "UPDATE students SET first_name='Emil', last_name='Johansson' WHERE id=42;"
