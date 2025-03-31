#!/usr/bin/python

import sys
import re
first = True
for line in sys.stdin:
    if first == True:
        first = False
        continue
    li = line.split(",")

    if len(li) < 5 or li[0] == 'Date':
        continue
    
    year = li[0][:4]
    quantity = li[4]
    
    if year == '' or quantity == '':
        continue

    print(f"{year}\t{quantity}\t1")
