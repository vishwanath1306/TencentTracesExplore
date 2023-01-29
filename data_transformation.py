import csv
import glob
import math
from copy import deepcopy
import logging
from multiprocessing import Pool
import time

from util import ( 
    get_array_indices, 
    load_logging_config, 
    get_op_file_name,  
    create_cvv_fd,
    get_split_file_name
)


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
    with Pool(10) as p:
        p.map(process_block_trace, files_list)

def write_only_cvv(filename):
    curr_time = round( time.time() * 1000)
    ip_file_fd = open(filename, "r")
    csv_reader = csv.reader(ip_file_fd)
    curr_file_name = get_split_file_name(filename)

    array_indices = get_array_indices()
    output_string = get_op_file_name(filename, op_folder)
    print(output_string)
    op_file_fd = open(output_string, "w")
    
    curr_cvv_set = set()

    for value, data in enumerate(csv_reader):
        
        if value % 1000000 == 0:
            logging.info(f"Completed lines: {value} from {filename}")
        
        curr_cvv = int(data[array_indices['volume_id']])
        if curr_cvv not in curr_cvv_set:            
            op_file_fd.write(f"{curr_cvv}\n")
            curr_cvv_set.add(curr_cvv)
      
    elapsed_time = round(time.time() * 1000) - curr_time
    logging.info(f"Finished file: {filename} in {elapsed_time} seconds")


def get_cvv_values(ip_folder, op_filename):
    ip_string = "{}/2*".format(ip_folder)
    files_list = glob.glob(ip_string)
    cvv_set = set()
    for file_ in files_list:
        file_fd = open(file_, 'r')
        value = file_fd.readlines()
        int_value = [int(x) for x in value]
        del value
        set_value = set(int_value)
        cvv_set.update(set_value)

    op_file_fd = open(op_filename, 'w')
    val_list = [f"{str(x)}\n" for x in cvv_set]
    op_file_fd.writelines(val_list)


def process_block_trace_cvv_split(filename):
    curr_time = round(time.time() *1000)
    file_fd = open(filename, 'r')
    csv_reader =  csv.reader(file_fd)
    cvv_fd_dict = dict()
    curr_file_name = get_split_file_name(filename)
    print(curr_file_name)
    array_indices = get_array_indices()
    for value, data in enumerate(csv_reader):
        curr_cvv = data[array_indices['volume_id']]
        print(curr_cvv)
    #     if value % 1000000 == 0:
    #         logging.info("Completed file: {} and {} rows".format(filename, value))
        
    #     curr_cvv = data[array_indices['volume_id']]
    #     if curr_cvv not in cvv_fd_dict.keys():
    #         try: 
    #             cvv_fd_dict[curr_cvv] = create_cvv_fd(op_folder, curr_cvv, curr_file_name)    
    #         except FileExistsError as e:
    #             pass

    #     cvv_fd_dict[curr_cvv].writerow(data)
    #     if(value == 3000000):
    #         break
    # elapsed_time = round(time.time() * 1000) - curr_time
    # logging.info(f"Finished file: {filename} in {elapsed_time} seconds")


def convert_to_cvv(input_folder):
    load_logging_config()
    ip_string = "{}/2*".format(input_folder)
    files_list = glob.glob(ip_string)

    with Pool(10) as p:
        p.map(write_only_cvv, files_list)
    # process_block_trace_cvv_split(files_list[0])
    # write_only_cvv(files_list[0])
    # with Pool(1) as p:
    #     p.map(process_block_trace_cvv_split, files_list)
    # process_block_trace_cvv_split(files_list[0])


if __name__ == "__main__":
    import argparse
    argument_parser = argparse.ArgumentParser()

    argument_parser.add_argument("ip_folder", help="Enter the folder in which you want to read the input files.")
    argument_parser.add_argument("op_folder", help="Enter the output folder for the trace files.")
    argument_parser.add_argument("operation", help="Enter the operation: 0 for block; 1 for cvv; 2 for extracting cvv")

    args = argument_parser.parse_args()

    ip_folder = args.ip_folder
    op_folder = args.op_folder
    operation = int(args.operation)

    if operation == 0:
        convert_to_4k(input_folder=ip_folder)
    elif operation == 1:
        convert_to_cvv(input_folder=ip_folder)
    elif operation == 2:
        get_cvv_values(ip_folder=ip_folder, op_filename=op_folder)
    else:
        print("Wrong operation")
    