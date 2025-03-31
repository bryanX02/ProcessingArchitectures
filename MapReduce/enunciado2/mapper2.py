#!/usr/bin/python

import sys

for line in sys.stdin:

    # Split the line into words
    words = line.split(' ')
    
    # Print the url and the number 1
    print(words[6] + "\t1")
