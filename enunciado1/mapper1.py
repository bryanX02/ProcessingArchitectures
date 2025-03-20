#!/usr/bin/python

import sys
import re

if len(sys.argv) == 2:
    for line in sys.stdin:
        words = re.sub(r'\W+', ' ', line).lower().split()
        if sys.argv[1].lower() in words:
            print(line.lower())
