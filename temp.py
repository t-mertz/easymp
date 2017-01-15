"""
(c) 2015-2016 Thomas Mertz
"""


import random
import os

# List of integers <-> ASCII characters allowed in filenames (letters, numbers, several other characters)
ASCII_LIST = list(range(65, 91)) + list(range(97, 123)) + list(range(48, 58))


def rnd_string(maxlength=20):
	"""
	Creates a random string of characters from ASCII_LIST.
	
	maxlength : maximal length of the string. Length is random and on
				average maxlength/2.
	""" 
	length = int(random.random() * maxlength)
	filename = ""
	for idx in range(length):
		filename += chr(random.choice(ASCII_LIST))
	
	return filename

def new_temp_file(prefix='tmp', suffix=''):
	"""
	Create a new random filename that doesn't exist yet suitable for
	temporary files.
	
	prefix : string to be added to the front of a random string
	suffix : string to be added to the end of a random string
	"""
	filename = prefix + rnd_string() + suffix
	while os.access(filename, os.F_OK):
		filename = prefix + rnd_string() + suffix
	
	return filename