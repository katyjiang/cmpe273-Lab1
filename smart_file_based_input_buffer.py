""" A smart file based input buffer.
Constructor parameter is a file (a path to the file in disk, type string),
when calling getNext(), it will return next available int value from the file,
when reach to the end of the file, return None.
"""

from itertools import islice

class SmartFileBasedInputBuffer:
	
	# constructor
	def __init__(self, f):
		self.opened_file = open(f, 'r')
		self.current_index = 0
		self.isEof = False # a variable denote whether we already read to the End of File
		self.small_buffer = self.__next_10_int(self.opened_file)
		
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
	def getNext(self):
		if self.isEof:
			return None

		if self.current_index < len(self.small_buffer):
			ret = self.small_buffer[self.current_index]
			self.current_index = self.current_index + 1
			return ret

		# Else lets try to read next 10 int from disk to memory
		self.small_buffer = self.__next_10_int(self.opened_file)
		if len(self.small_buffer) == 0:
			self.isEof = True # Yes, we already read all values from file.
			self.opened_file.close()
			return None

		# If we reach here, then means we successfully read new values from disk
		self.current_index = 1
		return self.small_buffer[0]

	# A private function.
	def __next_10_int(self, opened_file):
		return [int(line) for line in islice(opened_file, 10)]

# For interactive testing only
if __name__ == "__main__":
	buffer = SmartFileBasedInputBuffer("./input/unsorted_1.txt")
	while True:
		a = buffer.getNext()
		if a is None:
			break
		print(a)
