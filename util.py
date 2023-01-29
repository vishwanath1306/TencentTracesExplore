import datetime
import logging
import pytz


def get_array_indices():

    trace_dict = {
        "timestamp": 0,
        "offset": 1,
        "size": 2,
        "type": 3,
        "volume_id": 4
    }

    return trace_dict


def get_split_file_name(ip_file):
    splitted_string = ip_file.split('/')[-1]
    return splitted_string


def create_cvv_fd():
    return "Hello World"


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