"""
Copyright (c) 2015 Thomas Mertz

Module easymp defines abstract class Pool and classes FakePool and AutoPool.
Wraps multiprocessing.Pool class for smaller user code. Implements optional
progress tracking and remaining time estimation. Can be useful if many
similar calculations have to be computed.
"""
import sys,os,os.path,socket
if socket.gethostname() == 'THOMAS-PC':
	sys.path.append(os.path.expanduser('~/SkyDrive/codes/progress/py'))
elif socket.gethostname() == 'SURFACE':
	sys.path.append(os.path.expanduser('~/OneDrive/codes/progress/py'))
elif socket.gethostname() == 'DESKTOP-HTM8CHO':
	sys.path.append(os.path.expanduser('~/OneDrive/codes/progress/py'))

import math
import multiprocessing
import progress, lacommon
import time
import warnings
import mybuffer
import logging

logger = logging.getLogger()
h = logging.StreamHandler(sys.stdout)
logger.addHandler(h)
logger.setLevel(logging.INFO)

DEBUG = False

__version__ = '0.13'

def dummy_function(*args):
	return args
	
#def dummy_function(*args):
#	return args[0]

class Pool(object):
	"""
	Base class for data type Pool.
	"""
	def __init__(self):
		pass
	
	def close(self):
		if hasattr(self, '_pool'):
			self._pool.close()
		
	def join(self):
		if hasattr(self, '_pool'):
			self._pool.join()
		
	def terminate(self):
		if hasattr(self, '_pool'):
			self._pool.terminate()
			
	def apply_async(self):
		pass
		
class FakePool(Pool):
	"""
	Wrapper for easy coexisting implementations of serial and parallel
	computations.
	Inspired by Daniel Cocks.
	"""
	
	def apply_async(self, func, args):
		self._func = func
		self._args = args
	
	def get(self):
		return self._func(*self._args)
		
	def map_async(self, func, args):
		res = []
		for arg in args:
			if hasattr(self, '_common_args'):
				arg = arg + tuple(self._common_args)
			res += [func(*arg)]
		
		return res
		
	def map_async_tracked(self, func, args):
		"""
		Take statistics of job execution times.
		"""
		res = []
		i = 0
		for arg in args:
			if hasattr(self, '_common_args'):
				arg = arg + tuple(self._common_args)
			i += 1
			t0 = time.time()
			res += [func(*arg)]
			logging.info("Time for task {0:d}/{1:d}: {2:.2f}s".format(i, len(args), time.time() - t0))
		
		return res
	
	def set_common_args(self, args):
		self._common_args = args
	
	#def map_async_tracked(self, func, args):
	#	self.map_async(func, args)
	

class AutoPool(Pool):
	"""
	Wrapper for multiprocessing.Pool simplifies client code.
	Opening and closing of the Pool is implemented in the constructor/
	destructor of the class, the function to be executed can be passed
	as usual.
	Exceptions are reraised.
	"""
		
	def __init__(self, **kwargs):
		"""
		Constructor for the class.
		
		kwargs
		ncpus   : number of cpus/worker processes
		timeout : time (seconds) until multiprocessing.get returns 
		          TimeoutError
		"""
		try:
			self._ncpus = multiprocessing.cpu_count()
		except:
			self._ncpus = 2
			warnings.warn("Could not determine number of CPUs installed. Using default: 2",\
							UserWarning, stacklevel=2)
		self._timeout = 1
		self._starttime = time.time()
		self._common_args = None
		self._buffersize = 100
		
		for kwd in kwargs:
			if kwd == 'ncpus':
				if kwargs[kwd] > 0:
					tmp = int(kwargs[kwd])
					if self._ncpus > tmp:
						warnings.warn("Selected less CPUs than number installed. Installed: {}, Selected: {}".format(self._ncpus, tmp),\
							UserWarning, stacklevel=2)
					self._ncpus = tmp
				else:
					raise TypeError("ncpus must be positive number.")
			elif kwd == 'timeout':
				if kwargs[kwd] > 0:
					self._timeout = kwargs[kwd]
				else:
					raise TypeError("ncpus must be positive number.")
			elif kwd == 'buffersize':
				if int(kwargs[kwd]) > 0:
					self._buffersize = int(kwargs[kwd])
				else:
					raise TypeError("buffersize must be positive number.")
		
		self._res = []
		try:
			self._pool = multiprocessing.Pool(self._ncpus)
			self._state = 'ACTIVE'
		except:
			raise
		
	def info(self):
		"""
		Print information about number of CPUs, timeout settings and state.
		"""
		lifetime = lacommon.format_time(time.time() - self._starttime, \
										format_spec='')
		out_txt = "INFO:\n"
		out_txt += "{0:15s} {1}\n".format("ncpus", self._ncpus)
		out_txt += "{0:15s} {1}\n".format("timeout", self._timeout)
		out_txt += "{0:15s} {1}\n".format("state", self._state)
		out_txt += "{0:15s} {1}\n".format("time live", lifetime)
		
		sys.stdout.write(out_txt)
		sys.stdout.flush()
		
	def state(self):
		"""
		Return the state of the current instance.
		"""
		return self._state
	
	def set_common_args(self, args):
		"""
		Set common arguments.
		"""
		self._common_args = tuple(args)
		
	def del_common_args(self):
		"""
		Delete common arguments.
		"""
		self.common_args = None
	
	def _add_common_args(self, args):
		"""
		Add the common args to the end of the argument list.
		"""
		if not lacommon.isiterable(args):
			args = (args,)
		if self._common_args is not None:
			return args + self._common_args
		else:
			return args
	
	def apply_async(self, f, args):
		"""
		Apply a function to a given set of arguments. Length of the argument
		list and number of arguments of the function must agree.
		For asynchronous application of a function on multiple sets of input
		parameters see func::map_async or `func::map_async_tracked`.
		
		f    : function, must be picklable
		args : argument list
		"""
		args = self._add_common_args(args)
		try:
			self._res += [self._pool.apply_async(f, args)]
		except:
			pass
			raise Exception("Exception occurred.")
	
	def map_async(self, f, args):
		"""
		Apply a function to all elements in the argument list asynchronously.
		
		f    : function, must be picklable
		args : argument list
		"""
		assert len(args) > 0, "Argument list cannot be empty."
		ntasks = len(args)
		#ntasks_per_cpu = ntasks // self._ncpus
		
		if DEBUG:
			print(args)
		self._res = []
		
		try:
			for task in args:
				#task = self._add_common_args(task) # moved to apply_async()
				self.apply_async(f, task)
			
			res =  self.get()
			
		except TypeError:
			raise
		
		return res
		
	def map_async_tracked(self, f, args):
		"""
		Apply a function to all elements in the argument list asynchronously,
		while the progress is being tracked and printed to <stdout>. The optimal
		partition into parallel executed chunks is determined automatically based
		on the print_interval.
		
		f    : function, must be picklable
		args : argument list
		"""
		assert len(args) > 0, "Argument list cannot be empty."
		ntasks = len(args)
		#ntasks_per_cpu = ntasks // self._ncpus
		
		print_interval = 0.1
		ntasks_per_step = max(int(math.floor(print_interval * ntasks)), 1)
		
		#fill_tasks = self._ncpus if (ntasks_per_step % self._ncpus) > 0 else 0
		fill_tasks = self._ncpus - ntasks_per_step % self._ncpus if (ntasks_per_step % self._ncpus) > 0 else 0
		ntasks_per_step = ntasks_per_step + fill_tasks # ntasks_per_step >= _ncpus
		
		self._tracker = progress.Progress(ntasks)
		
		if DEBUG:
			print("ntasks {}".format(ntasks))
			print("ncpus {}".format(self._ncpus))
			print("ntasks_per_step {}".format(ntasks_per_step))
		
		remaining = ntasks
		cur_task = 0
		res = []
		
		res_buffer = mybuffer.TempBuffer(self._buffersize)
		
		while (remaining > 0):
			self._res = []
			
			#if remaining >= self._ncpus:
			if remaining >= ntasks_per_step:
				ntasks_scheduled = ntasks_per_step
			else:
				ntasks_scheduled = remaining
				
			if DEBUG:	
				print("remaining {}".format(remaining))
				print("cur_task {}".format(cur_task))
				print("ntasks_scheduled {}".format(ntasks_scheduled))
				
			self._tracker.begin()
			cur_task_list = args[cur_task:cur_task+ntasks_scheduled]
			for task in cur_task_list:
				#task = self._add_common_args(task) # moved to apply_async()
				#print(task)
				[self.apply_async(f, task)]
			
			try:
				#cur_timeout = self._timeout * ntasks_scheduled // self._ncpus
				new_res = self.get()
				
				res_buffer.put_sequence(new_res)
				
				res += new_res
			except:
				print("tasks: ", cur_task_list)
				raise
			
			self._tracker.record(ntasks_scheduled)
			
			cur_task += ntasks_scheduled
			remaining -= ntasks_scheduled
			
		assert remaining == 0, "Not all jobs were executed."
		assert cur_task == ntasks, "Not all jobs were executed."
		
		self._tracker.report()
		
		return res
		
	def get(self, timeout=None):
		"""
		Waits and obtains the results of the current tasks form the worker processes.
		"""
		if timeout is None:
			timeout = self._timeout
		try:
			temp_res = [res.get(timeout=timeout) for res in self._res]
			
			self._res = []
		
			return temp_res
		except multiprocessing.TimeoutError:
			pass
			self.cleanup()
			raise multiprocessing.TimeoutError("Process did not respond within " \
				+"{0} seconds.".format(timeout))
		except KeyboardInterrupt:
			self.cleanup()
			raise KeyboardInterrupt("Keyboard Interrupt - aborting.")
		except:
			pass
			self.cleanup()
			raise
	
	def get_tracked(self):
		"""
		Waits and obtains the results of the tasks form the worker processes.
		Implements progress tracking. In order to get the best accuracy,
		`get_tracked()` should be called right after `apply_async()`.
		"""
		assert len(res) > 0, "No pending tasks."
		ntasks = len(res)
		self._tracker = progress.Progress(ntasks)
		
				
		print_interval = 0.1
		ntasks_per_step = max(int(math.floor(print_interval * ntasks)), 1)
		
		fill_tasks = self._ncpus if (ntasks_per_step % self._ncpus) > 0 else 0
		ntasks_per_step = ntasks_per_step + fill_tasks
		
		remaining = ntasks
		cur_task = 0
		temp_res = []
		try:
			while (remaining > 0):
				if remaining >= self._ncpus:
					ntasks_scheduled = ntasks_per_step
				else:
					ntasks_scheduled = remaining
					
				if DEBUG:
					print("remaining {}".format(remaining))
					print("cur_task {}".format(cur_task))
					print("ntasks_scheduled {}".format(ntasks_scheduled))
							
				self._tracker.begin()
				for task in range(cur_task, cur_task+ntasks_scheduled):
					temp_res += self._res[task].get()
				
				self._tracker.record(ntasks_scheduled)
				
				cur_task += ntasks_scheduled
				remaining -= ntasks_scheduled
				
			assert remaining == 0, "Not all jobs were executed."
			assert cur_task == ntasks, "Not all jobs were executed."
			
			self._tracker.report()
				
			self._res = []
			
			return temp_res
		except multiprocessing.TimeoutError:
			pass
			self.cleanup()
			raise multiprocessing.TimeoutError("Process did not respond within \
				{0} seconds.".format(self._timeout))
		except KeyboardInterrupt:
			self.cleanup()
			raise KeyboardInterrupt("Keyboard Interrupt - aborting.")
		except:
			pass
			self.cleanup()
			raise
		
	
	def cleanup(self):
		"""
		Close the pool, terminate jobs and join processes.
		"""
		if self._state == 'ACTIVE':
			self._pool.close()
			self._pool.terminate()
			self._pool.join()
			self._state = 'INACTIVE'
	
	def revive(self, verbose=False):
		"""
		Revive Pool, return to initial state.
		
		verbose : re-raise exception True/False
		"""
		try:
			self._pool = multiprocessing.Pool(self._ncpus)
			self._state = 'ACTIVE'
			return 0
		except:
			if verbose:
				raise
			return 1
	
	def __del__(self):
		"""
		Destructor for the class.
		"""
		self.cleanup()
		
def map_async_tracked(f, args, processes=1, timeout=-1):
	"""
	Apply a function to all elements in the argument list asynchronously,
	while the progress is being tracked and printed to <stdout>. The optimal
	partition into parallel executed chunks is determined automatically based
	on the print_interval.
	Static equivalent to AutoPool.map_async_tracked method.
	
	f    : function, must be picklable
	args : argument list
	"""
	p = AutoPool(ncpus=processes, timeout=timeout)
	
	res = p.map_async_tracked(f, args)
	
	del p
	
	return res

def load_buffer(filename):
	"""
	Load result from buffer.
	
	filename : name of the buffer file. Check your directory for a *.buf file
	"""
	with open(filename, 'r') as buf_file:
		data = pickle.load(buf_file)
	
	return data