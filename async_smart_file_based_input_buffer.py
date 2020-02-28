""" A smart file based input buffer.
Constructor parameter is a file (a path to the file in disk, type string),
when calling getNext(), it will return next available int value from the file,
when reach to the end of the file, return None.
This class is almost same as SmartFileBasedInputBuffer, the only thing I changed
in this file is to add async disk read while bufferred elements number is low.
But according to my test, this AsyncSmartFileBasedInputBuffer doesn't perform
better than SmartFileBasedInputBuffer, one reason could be the data set is 
too small.
"""

from itertools import islice
import asyncio
import sys, os

class AsyncSmartFileBasedInputBuffer:
	
	# constructor
	def __init__(self, f, loop):
		self.opened_file = open(f, 'r')
		self.loop = loop
		self.pending_read_task = self.loop.create_task(self.__next_10_int(self.opened_file)) 
		self.small_buffer = []
		self.isEof = False # a variable denote whether we already read to the End of File
	
	# destructor
	def __del__(self):
		try:
			self.opened_file.close()
		except: # catch all kind of errors
			pass # do nothing

	# Implement this function to make heapq works in Python 3+
	def __eq__(self, other):
		# We just compare identity
		return id(self) == id(other)

	# Implement this function to make heapq works in Python 3+
	def __lt__(self, other):
		# We just compare identity
		return id(self) < id(other) 

	# A public function which return next int from the file
	# when read to the end of the file, return None.
	async def getNext(self):
		
		if len(self.small_buffer) == 0 and self.isEof:
			return None
	
		if self.pending_read_task is not None and self.pending_read_task.done():
			if len(self.pending_read_task.result()) == 0:
				self.isEof = True
			self.small_buffer.extend(self.pending_read_task.result())
			self.pending_read_task = None

		# If buffer still has many elements, then we just return the first one
		if len(self.small_buffer) > 3:
			return self.small_buffer.pop(0)

		# Otherwise we could the same time schedule a disk read task.
		if len(self.small_buffer) > 0 :
			ret = self.small_buffer.pop(0)
			if self.pending_read_task is None and not self.isEof:
				self.pending_read_task = self.loop.create_task(self.__next_10_int(self.opened_file))
			return ret
		
		# When we reached here, means there is no element in self.small_buffer
		await asyncio.wait([self.pending_read_task])
		if len(self.pending_read_task.result()) == 0:
			self.isEof = True
			self.pending_read_task = None
			return None
		else:
			self.small_buffer.extend(self.pending_read_task.result())
			self.pending_read_task = None
			return self.small_buffer.pop(0)
		
	# A private function.
	async def __next_10_int(self, opened_file):
		return [int(line) for line in islice(opened_file, 20)]

# For interactive testing only
if __name__ == "__main__":
	try:
		loop = asyncio.get_event_loop()
		#loop.set_debug(True)
		buffer = AsyncSmartFileBasedInputBuffer("./input/unsorted_1.txt", loop)
		while True:
			a = buffer.getNext()
			if a is None:
				break
			print(a)
	except: # Catch all kind of errors
		pass # For now do nothing
	finally:
		loop.close()
