import csv
import datetime
import logging
import pytz
import os


def get_array_indices():

    trace_dict = {
        "timestamp": 0,
        "offset": 1,
        "size": 2,
        "type": 3,
        "volume_id": 4
    }

    return trace_dict


def get_write_file_fd(cvv_dict, actual_file):
    cvv_fd_dict = dict()
    for key in cvv_dict.keys():
        actual_path = f"{cvv_dict[key]}{actual_file}.csv"
        cvv_fd = open(actual_path, 'w')
        cvv_fd_dict[str(key)] = csv.writer(cvv_fd)
    
    return cvv_fd_dict



def create_op_folder(op_folder):
    cvv_folder = '/research/oathkeeper/cvv_id_unique.out'
    cvv_fd = open(cvv_folder, 'r')
    data = cvv_fd.readlines()
    cvv_data = [int(x) for x in data]
    cvv_dict = dict()
    for cvv in cvv_data:
        folder_path = f"{op_folder}{cvv}/"
        if not os.path.exists(folder_path):
            os.mkdir(folder_path)
        cvv_dict[cvv] = folder_path
    print("Finished creating folders")
    return cvv_dict

def get_split_file_name(ip_file):
    splitted_string = ip_file.split('/')[-1]
    return splitted_string


def get_op_file_name(ip_file, op_folder):
    splitted_string = ip_file.split('/')[-1]
    print(splitted_string)
    op_file = "{}{}.csv".format(op_folder, splitted_string)
    return op_file


def load_logging_config():
    time_zone = pytz.timezone('US/Eastern')
    curr_date = datetime.datetime.now(tz=time_zone)
    date_as_string = curr_date.strftime('%Y-%m-%d_%H-%M-%S')
    logging.basicConfig(filename="./{}/{}.log".format("log", date_as_string),
                            level=logging.INFO)