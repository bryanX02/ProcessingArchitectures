#!/usr/bin/python
 
import sys
import re

# filtering top 20 urls
def setup():
    list = []

def map():
    for line in sys.stdin:
        key, value = line.split('\t')

        #insert record in list sorted by f
        list.append((key, int(value)))
        list.sort(key=lambda x: x[1], reverse=True)
        #truncate list to top K
        if len(list) > 20:
            list.pop()

def cleanup():
    for value in list:
        print(value[0] + '\t' + str(value[1]))

