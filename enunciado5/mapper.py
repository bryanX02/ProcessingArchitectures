#!/usr/bin/python

import sys
import re
first = True
i = 0
for line in sys.stdin:
    if first == True:
        first = False
        continue
    li = line.strip().split(";")
    recclass = str(li[3])
    if li[4] == '':
        continue
    mass = float(li[4])
    
    print(f"{recclass}\t{mass}\t1")
