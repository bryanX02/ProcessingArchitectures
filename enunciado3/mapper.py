#!/usr/bin/python

import sys
import re
first = True
for line in sys.stdin:
    if first == True:
        first = False
        continue
    li = line.split(",")
    year = li[0][:4]
    quantity = li[4]
    
    print(f"{year}\t{quantity}\t1")
