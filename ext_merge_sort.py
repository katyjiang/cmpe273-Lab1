from os import listdir
import os
from os.path import isfile, join
import time
from heapq import heappush
from heapq import heappop
from smart_file_based_input_buffer import SmartFileBasedInputBuffer
import shutil # For deleting folder content

_input_folder = "./input/"
_output_folder = "./output/"
_tmp_folder = "./tmp/"
_final_sorted_result = "sorted.txt"
_execution_time = "time.txt"

def sort():

	create_folders()	

	# Mark the start time exclude the folder creation time.
	t_start = time.process_time()

	# Phase 1: sort files and save sorted result into a tmp folder
	unsorted_files = [f for f in listdir(_input_folder) if isfile(join(_input_folder, f))]
	for f in unsorted_files:
		sort_file_and_write_tmp(f)

	# Phase 2: a 10-way merge to merge all sorted result
	# I am going to use python standard lib heapq, if TA required,
	# I can implement a min-heap from scratch
	input_buffers = [SmartFileBasedInputBuffer(join(_tmp_folder, f)) for f in unsorted_files]
	pq = []
	with open(join(_output_folder, _final_sorted_result), 'w') as output_result:
		# We first push the first element of each buffer into priority queue
		for input_buffer in input_buffers:
			val = input_buffer.getNext()
			if val is not None:
				heappush(pq, (val, input_buffer))
		
		# Now we can extract top element from priority queue which is
		# guaranteed to be the smallest for now
		while len(pq) > 0:
			key, input_buffer = heappop(pq)
			output_result.write(str(key))
			output_result.write("\n")
			
			# Now push next element of buffer into the priority queue
			val = input_buffer.getNext()
			if val is not None:
				heappush(pq, (val, input_buffer))

	t_pass = time.process_time() - t_start
	with open(join(_output_folder, _execution_time), 'w') as time_txt:
		time_txt.write(str(t_pass) + " seconds")

	cleanup()
	

# A function used to create folders, delete existing files
# from folder if existed.
def create_folders():
	try:
		os.mkdir(_output_folder)
	except OSError:
		# do nothing
		pass

	try:
		os.mkdir(_tmp_folder)
	except OSError:
		for f in listdir(_tmp_folder):
			os.remove(join(_tmp_folder, f))

# A function used to sort a file in disk and write the sorted result into a file
# with same name in _tmp_folder
def sort_file_and_write_tmp(f):
	unsorted_array = []
	with open(join(_input_folder, f), 'r') as input:
		for line in input:
			unsorted_array.append(int(line))

	# For now I am using python builtin sort function
	# can re-implement sort function if TA really want it.
	unsorted_array.sort() # default sort in ascending order

	with open(join(_tmp_folder, f), 'w') as output:
		for number in unsorted_array:
			output.write(str(number))
			output.write("\n")	

# Clean temporary files
def cleanup():
	#for f in listdir(_tmp_folder):
	#	os.remove(join(_tmp_folder, f))
	#os.rmdir(_tmp_folder)
	shutil.rmtree(_tmp_folder)

	# Remove any pre-compiled python code
	for f in listdir("./"):
		if f.endswith(".pyc"):
			os.remove(join("./", f))
		elif os.path.isdir(join("./", f)) and "pycache" in f:
			shutil.rmtree(join("./", f))

if __name__ == "__main__":
	sort()
