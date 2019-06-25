# Multi Session Append Example
This is client example codes that builds a multi-session and performs append and retrieval repeatedly to verify that Machbase DB can reliably collect data from a large number of sensors.

# Contents
* crt.sql: DDL queries for creating tables
* append.c: Client sample code for testing multi-session append

# Compile
* make: create append program
* make clean: remove append program

# Append Program Argument
* session count: Number of sessions to create
* row count: Number of total rows to append
* port number: Machbase port number

# Test
```
make
# create table using crt.sql and machsql
./append 2000 10000000 5656
```

# Reference
https://www.machbase.com/kr/howtouse/?mod=document&uid=91
