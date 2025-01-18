import numpy as np
import requests
def dimensions_test(list):
    np_list = np.array(list)
    return np_list.ndim

def azure_instance_name(input_string):
    if input_string.startswith("Standard"):
        input_string = input_string[len("Standard"):]
        input_string = input_string.replace("_", " ")
        input_string += " spot"
    return input_string

def get_hour_combinations(duration):
    numbers = list(range(24)) # 0 to 23
    combinations = []

    for i in range(len(numbers)):
        m = 0
        combination = []
        while m < duration:
            combination.append(numbers[(i + m) % len(numbers)])
            m += 1
        combinations.append(combination)

    return combinations

def request_into_list():
    response = requests.post(
        "http://localhost:8080/simulate/aws?cloudletLength=10000",
        json={"key": "value"} # Request body
    )
    """
    {
        "instance_name": "t2.micro",
        "execution_time": 123.45
    }
    """
    for item in response.json():
        list.append([item[0], item[1]])

    return list

