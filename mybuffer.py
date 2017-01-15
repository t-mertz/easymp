"""
Defines a Buffer class for efficient storage to an output. FileBuffer can be
used for buffered output to a file. TempBuffer uses a temporary file, which
gets deleted when the buffer is closed.
"""
import temp
import myqueue as queue
import os

try:
	import pickle
	PICKLED = True
	
except ImportError:
	PICKLED = False


class Buffer(object):
	def empty(self):
		pass
	
	def full(self):
		pass
		
	def length(self):
		pass
		
	def put(self):
		pass
		
	def flush(self):
		pass
		
	
class FileBuffer(Buffer):
	def __init__(self, length=1):
		self.length = length
		self.queue = queue.Queue()
		self.filename = filename
		
	def empty(self):
		return self.queue.empty()
		
	def full(self):
		return self.queue.length == self.length
		
	def flush(self):
		if self.full():
			if not PICKLED:
				with open(self.filename, 'w') as f:
					while not self.queue.empty():
						f.write(self.queue.get())
			else:
				if os.access(self.filename, os.F_OK):
					unpcklr = pickle.Unpickler(open(self.filename, 'r'))
					data = unpcklr.load()
				else:
					data = []
				pcklr = pickle.Pickler(open(self.filename, 'w'))
				while not self.queue.empty():
					data.append(self.queue.get())
				pcklr.dump(data)
				
	def put(self, obj):
		self.queue.put(obj)
		self.flush()
		
	def put_sequence(self, seq):
		for obj in seq:
			self.queue.put(obj)
			self.flush()
	
class TempBuffer(FileBuffer):
	def __init__(self, length=1):
		self.filename = temp.new_temp_file() + ".buf"
		self.length = length
		self.queue = queue.Queue()
		
	def __del__(self):
		if os.access(self.filename, os.F_OK):
			os.unlink(self.filename)
		del self.queue