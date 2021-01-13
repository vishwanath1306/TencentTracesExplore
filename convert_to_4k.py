import csv
import glob
import math
from copy import deepcopy
import logging
from multiprocessing import Pool


from util import get_array_indices, load_logging_config, get_op_file_name


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

    logging.info("Currently processing: {}".format(filename))

    array_indices = get_array_indices()
    output_string = get_op_file_name(filename, op_folder)
    op_file_fd = open(output_string, "w")
    op_csv = csv.writer(op_file_fd)

    for value, data in enumerate(csv_data):
        
        if value % 1000000 == 0:
            logging.info("Finished file: {} and {} rows".format(filename, value))
        # print(data)

        size_ = int(data[array_indices["size"]])
        block = int(data[array_indices["offset"]])
        # print(block, size_)

        start_index = math.floor((block * 512) / 4096)
        end_index = math.floor(( (block * 512) + (size_ * 512) - 1) / 4096)

        # print(start_index, end_index)
        if start_index == end_index:
            block_list = deepcopy(data)
            block_list[array_indices["offset"]] = start_index
            if block_list[array_indices["type"]] == "0":
                block_list[array_indices["type"]] = "r"
            else:
                block_list[array_indices["type"]] = "w"

            block_list.pop(array_indices["size"])            
            op_csv.writerow(block_list)
            # print(block_list)

        else:    
            for val in range(start_index, end_index+1):
                block_list = deepcopy(data)
                block_list[array_indices["offset"]] = val
                if block_list[array_indices["type"]] == "0":
                    block_list[array_indices["type"]] = "r"
                else:
                    block_list[array_indices["type"]] = "w"
                block_list.pop(array_indices["size"])
                op_csv.writerow(block_list)
                # print(block_list)
        
        # print("###################")

        # if value == 20:
        #     break

    logging.info("Finished file: {}".format(filename))

def convert_to_4k(input_folder):
    
    load_logging_config()
    ip_string = "{}/2*".format(input_folder)
    files_list = glob.glob(ip_string)
    print(files_list)
    # for file_ in files_list:
    #     process_block_trace(filename=file_)
        # break
    with Pool(2) as p:
        p.map(process_block_trace, files_list)

if __name__ == "__main__":
    import argparse
    argument_parser = argparse.ArgumentParser()

    argument_parser.add_argument("ip_folder", help="Enter the folder in which you want to read the input files.")
    argument_parser.add_argument("op_folder", help="Enter the output folder for the trace files.")

    args = argument_parser.parse_args()

    ip_folder = args.ip_folder
    op_folder = args.op_folder

    convert_to_4k(input_folder=ip_folder)