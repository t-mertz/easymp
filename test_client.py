"""
Copyright (c) 2015 Thomas Mertz

Test client for easymp module.
"""

from __future__ import print_function
import easymp
import time, sys

def dummy_function(x):
	time.sleep(0.1)
	return x


if __name__ == "__main__":
	
	N = 100
	ncpus = 4
	
	print("{0:14s}{1}".format("Version", easymp.__version__))
	print("{0:14s}{1}".format("Platform", sys.platform))
	print("{0:14s}{1}".format("#Processes", ncpus))
	print("{0:14s}{1}\n".format("#Tasks", N))
	
	print("Calculating reference test...")
	check = [dummy_function(x) for x in (1,2,3)*N]
	print("done.\n")
	
	p = easymp.AutoPool(timeout=5, ncpus=ncpus, buffersize=10)
	
	print("Testing apply_async(f, (1,)):")
	p.apply_async(dummy_function, (1,))
	res1 = p.get()
	print("Returns {}\n".format(res1))
	if res1[0] == check[0]:
		print("Passed.\n")
		flag1 = True
	else:
		print("Failed.\n")
		flag1 = False
	
	print("Testing map_async(f, (1,2,3)*N):")
	res2 = p.map_async(dummy_function, (1,2,3)*N)
	if N < 5:
		print("Returns {}\n".format(res))
	else:
		print("Result suppressed.\n")
	if res2 == check:
		print("Passed.\n")
		flag2 = True
	else:
		print("Failed.\n")
		flag2 = False
		
	print("Testing map_async_tracked(f, (1,2,3)*N):")
	res3 = p.map_async_tracked(dummy_function, (1,2,3)*N)
	if N < 5:
		print("Returns {}\n".format(res))
	else:
		print("Result suppressed.\n")
	if res3 == check:
		print("Passed.\n")
		flag3 = True
	else:
		print("Failed.\n")
		flag3 = False
	
	
	if flag1 and flag2 and flag3:
		print("\nTEST PASSED.\n")
	else:
		print("\nTEST FAILED.\n")
		print(check)
		print(res2)
		print(res3)
		
	p.info()
	
	del p