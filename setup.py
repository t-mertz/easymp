#!/usr/bin/env python

from distutils.core import setup
"""
try:
	import progress
except ImportError:
	print("Could not import progress. Install progress first")
"""

setup(
	name='easymp',
	version='0.13',
	author='Thomas Mertz',
	url='thomasmertz.eu',
	license='GNU GPL',
	py_modules=['easymp', 'mybuffer', 'myqueue', 'temp', 'test_client'],
)
