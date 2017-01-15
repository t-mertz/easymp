"""
Defines a standard FIFO Queue class.

TODO: Implement in C++ for better performance!
"""

class BaseQueue(object):
	"""
	Base class for queues.
	Queues derived from this base class must implement the
	following methods:
	
	* put()
	* get()
	* empty()
	"""
	def put(self):
		pass
		
	def get(self):
		pass
		
	def empty(self):
		pass

class QueueItem(object):
	"""
	Helper class for queue items.
	Improves memory cost by having only one queue object,
	all elements of the queue are of class QueueItem. (Recursion
	of Queue objects inefficient.)
	Necessary due to the absence of pointers in python.
	"""
	def __init__(self, item):
		self.item = item
		self.next = None
		
class Queue(BaseQueue):
	"""
	FIFO Queue class.
	"""
	def __init__(self, obj=None):
		self.head = None
		self.tail = self
		self.next = None
		self.length = 0
		
	def put(self, obj):
		"""
		Insert object at the end of the queue.
		"""
		new_tail = QueueItem(obj)
		self.tail.next = new_tail
		self.tail = new_tail
		if self.head is None:
			self.head = new_tail
		self.length += 1
		
	def get(self):
		"""
		Extract first object from the queue.
		"""
		if self.head is not None:
			old_head = self.head
			obj = old_head.item
			self.head = old_head.next
			del old_head
			self.length -= 1
			return obj
		else:
			raise Exception("Queue is empty")
			
	def empty(self):
		"""
		Check if queue is empty.
		
		Returns boolean
		"""
		return True if self.head is None else False
		
	def __str__(self):
		out = '<--['
		if self.head is not None:
			cur = self.head
			while cur.next is not None:
				out += str(cur.item) + ", "
				cur = cur.next
			out += str(cur.item)
		out += ']<--'
		return out
		
	def __repr__(self):
		out = '<--['
		if self.head is not None:
			cur = self.head
			while cur.next is not None:
				out += str(cur.item) + ", "
				cur = cur.next
			out += str(cur.item)
		out += ']<--'
		return out