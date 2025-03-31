#!/usr/bin/python

import sys

first = True
for line in sys.stdin:
    if first == True:
        first = False
        continue
    li = line.strip().split(",")
    movieId = int(li[1])
    rating = float(li[2])
    
    print(f"{movieId}\t{rating}\t1")
