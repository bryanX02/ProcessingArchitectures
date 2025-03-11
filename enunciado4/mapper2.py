#!/usr/bin/python

import sys
import re
for line in sys.stdin:
    li = line.strip().split("\t")
    movieId = int(li[0])
    rating = float(li[1])
    if rating <= 1:
        print(f"{movieId}\tRange 1\t")
    elif rating <= 2:
        print(f"{movieId}\tRange 2\t")
    elif rating <= 3:
        print(f"{movieId}\tRange 3\t")
    elif rating <= 4:
        print(f"{movieId}\tRange 4\t")
    elif rating <= 5:
        print(f"{movieId}\tRange 5\t")
