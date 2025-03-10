#!/usr/bin/python

import sys
import re

if len(sys.argv) == 2:
    for line in sys.stdin:
        if sys.argv[1] in line.lower():
            print(line.lower())
