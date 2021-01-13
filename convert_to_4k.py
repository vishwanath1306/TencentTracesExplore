import csv
import glob
import math
from copy import deepcopy

from util import get_array_indices


'''
Convert the offset to bytes by multiplying with 512. 


start_page_index = math.floor((offset * 512) / 4096)

end_page_index = math.floor(((offset * 512) + (size * 512) - 1) / 4096)


spi --> x
epi --> y

(t, x, r/w, cvv)
...
(t, y, r/w, cvv)


'''
def process_block_trace(filename):
    ip_file_fd = open(filename, "r")
    csv_data = csv.reader(ip_file_fd)


    array_indices = get_array_indices()
    
    
    for value, data in enumerate(csv_data):

        print(data)

        size_ = int(data[array_indices["size"]])
        block = int(data[array_indices["offset"]])
        # print(block, size_)

        start_index = math.floor((block * 512) / 4096)
        end_index = math.floor(( (block * 512) + (size_ * 512) - 1) / 4096)

        if start_index == end_index:
            block_list = deepcopy(data)
            
        # for val in range(start_index, end_index):
            # print(val, end=" ") 

        print()
        if value == 11:
            break


def convert_to_4k(input_folder):
    
    ip_string = "{}/2*".format(input_folder)
    files_list = glob.glob(ip_string)
    print(files_list)
    for file_ in files_list:
        process_block_trace(filename=file_)

if __name__ == "__main__":
    import argparse
    argument_parser = argparse.ArgumentParser()

    argument_parser.add_argument("ip_folder", help="Enter the folder in which you want to read the input files.")
    argument_parser.add_argument("op_folder", help="Enter the output folder for the trace files.")

    args = argument_parser.parse_args()

    ip_folder = args.ip_folder
    op_folder = args.op_folder

    convert_to_4k(input_folder=ip_folder)