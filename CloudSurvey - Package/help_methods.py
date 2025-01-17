import numpy as np

def dimensions_test(list):
    np_list = np.array(list)
    return np_list.ndim

def azure_instance_name(input_string):
    if input_string.startswith("Standard"):
        input_string = input_string[len("Standard"):]
        input_string = input_string.replace("_", " ")
        input_string += " spot"
    return input_string

