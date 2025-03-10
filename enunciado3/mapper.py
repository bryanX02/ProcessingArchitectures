#!/usr/bin/python

import sys
import re

for line in sys.stdin:
    li = line.split(",")
    year = li[0][:4]
    quantity = li[4]
    
    print(f"{year}\t{quantity}\t1")
