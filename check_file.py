#!/usr/bin/env python
import hashlib


def get_file_sha256(file):
	m = hashlib.sha256()
	with open(file, mode='rb') as f:
		while True:
			data = f.read(10240)
			if not data:
				break
			m.update(data)
	return m.hexdigest().upper()
 

 

 

 

 
 
 
		
 
