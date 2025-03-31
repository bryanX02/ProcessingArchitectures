#!/usr/bin/python

import sys
import re

# 2. Count URL Access Frequency
# The Common Log Format (CLF) is a standardized text file format used by web servers when generating access logs.
# Find the 20 most accessed URLs in a web server from its access log. Ignore the query component of the URLs, which is preceded by a question mark.


for line in sys.stdin:

    # Split the line into words
    words = line.split(' ')
    
    # Print the url and the number 1
    print(words[6] + "\t1")