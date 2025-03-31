#!/usr/bin/python

import sys

key =""
prev=""
suma = 0
count = 0
for line in sys.stdin:
	li = line.strip().split("\t")
	key = li[0]
	value = (float(li[1]), int(li[2]))
	if key != prev:
		if prev!="":
			print(f"{prev}\t{suma/count}\t{count}")
		prev = key
		count = 0
		suma = 0

	suma += value[0]*value[1]
	count += value[1]

if prev != "":
	print(f"{prev}\t{suma/count}\t{count}")	
