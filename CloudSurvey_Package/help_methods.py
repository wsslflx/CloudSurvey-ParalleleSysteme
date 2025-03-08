import numpy as np
import requests
def dimensions_test(list):
    """
    Builds and solves a linear model picking exactly ONE combination of:
      (r1, r2, i, s, p)

    Where:
      - storage is in region r1 (with instance i, parallel factor p),
      - transfer is (r1->r2),
      - compute is in region r2 (with instance i, parallel factor p),
        starting time s.

    The total cost = storage_cost_map[r1, i, p]
                   + transfer_cost_map[r1, r2]
                   + compute_cost_map[r2, i, s, p][0][1]
    Exactly one tuple is chosen (x=1), to minimize total cost.
    """
    np_list = np.array(list)
    return np_list.ndim

def azure_instance_name(input_string):
    """
        Builds and solves a linear model picking exactly ONE combination of:
          (r1, r2, i, s, p)

        Where:
          - storage is in region r1 (with instance i, parallel factor p),
          - transfer is (r1->r2),
          - compute is in region r2 (with instance i, parallel factor p),
            starting time s.

        The total cost = storage_cost_map[r1, i, p]
                       + transfer_cost_map[r1, r2]
                       + compute_cost_map[r2, i, s, p][0][1]
        Exactly one tuple is chosen (x=1), to minimize total cost.
        """
    if input_string.startswith("Standard"):
        input_string = input_string[len("Standard"):]
        input_string = input_string.replace("_", " ")
        input_string += " spot"
    return input_string

def get_hour_combinations(duration):
    """
        Builds and solves a linear model picking exactly ONE combination of:
          (r1, r2, i, s, p)

        Where:
          - storage is in region r1 (with instance i, parallel factor p),
          - transfer is (r1->r2),
          - compute is in region r2 (with instance i, parallel factor p),
            starting time s.

        The total cost = storage_cost_map[r1, i, p]
                       + transfer_cost_map[r1, r2]
                       + compute_cost_map[r2, i, s, p][0][1]
        Exactly one tuple is chosen (x=1), to minimize total cost.
    """
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
    """
        Builds and solves a linear model picking exactly ONE combination of:
          (r1, r2, i, s, p)

        Where:
          - storage is in region r1 (with instance i, parallel factor p),
          - transfer is (r1->r2),
          - compute is in region r2 (with instance i, parallel factor p),
            starting time s.

        The total cost = storage_cost_map[r1, i, p]
                       + transfer_cost_map[r1, r2]
                       + compute_cost_map[r2, i, s, p][0][1]
        Exactly one tuple is chosen (x=1), to minimize total cost.
    """
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

def formatting_compute_cost_multiple(single_cost):
    """
        Builds and solves a linear model picking exactly ONE combination of:
          (r1, r2, i, s, p)

        Where:
          - storage is in region r1 (with instance i, parallel factor p),
          - transfer is (r1->r2),
          - compute is in region r2 (with instance i, parallel factor p),
            starting time s.

        The total cost = storage_cost_map[r1, i, p]
                       + transfer_cost_map[r1, r2]
                       + compute_cost_map[r2, i, s, p][0][1]
        Exactly one tuple is chosen (x=1), to minimize total cost.
    """
    formatted_costs = []
    for single in single_cost:
        lowerLimitPrice = single[0][0]
        meanPrice = single[0][1]
        upperLimitPrice = single[0][2]
        starting_time = single[0][3]
        duration = single[0][4]
        region = single[0][5]
        instance = single[1]
        formatted_costs.append(
            [
                lowerLimitPrice,
                meanPrice,
                upperLimitPrice,
                starting_time,
                duration,
                region,
                instance
            ]
        )
    return formatted_costs

def formatting_compute_cost_single(single_cost):
    """
        Builds and solves a linear model picking exactly ONE combination of:
          (r1, r2, i, s, p)

        Where:
          - storage is in region r1 (with instance i, parallel factor p),
          - transfer is (r1->r2),
          - compute is in region r2 (with instance i, parallel factor p),
            starting time s.

        The total cost = storage_cost_map[r1, i, p]
                       + transfer_cost_map[r1, r2]
                       + compute_cost_map[r2, i, s, p][0][1]
        Exactly one tuple is chosen (x=1), to minimize total cost.
        """
    flat_list = [element for item in single_cost for element in (item if isinstance(item, list) else [item])]
    return flat_list

def generate_output_storage(total_cost, single_costs, storage_cost, konfidenzgrad, multiple, provider):
    """
        Builds and solves a linear model picking exactly ONE combination of:
          (r1, r2, i, s, p)

        Where:
          - storage is in region r1 (with instance i, parallel factor p),
          - transfer is (r1->r2),
          - compute is in region r2 (with instance i, parallel factor p),
            starting time s.

        The total cost = storage_cost_map[r1, i, p]
                       + transfer_cost_map[r1, r2]
                       + compute_cost_map[r2, i, s, p][0][1]
        Exactly one tuple is chosen (x=1), to minimize total cost.
    """
    if provider == "Azure":
        currency = "€"
    else:
        currency = "$"

    print("Total Costs for " + provider + " :")
    print("Expected Costs: " + str(total_cost[1]) + " " + currency)
    print("Price Range: " + str(total_cost[0]) + " " + currency + " - " + str(total_cost[2]) + currency + " in " + str(konfidenzgrad) + "% of cases")
    print("Expected Time needed: " + str(total_cost[3]) + " hours")
    print()
    print("Storage Cost: ")
    print("Expected Costs: " + str(storage_cost[0]) + " " + currency)
    print("Best Region: " + str(storage_cost[1]))
    print()
    if multiple:
        for index, job in enumerate(single_costs):
            print("Jobs #" + str(index))
            print("Best Instance: " + str(job[6]) + " in Region: " + str(job[5]))
            print("Expected Price: " + str(job[1]) + " " + currency)
            print("Price Range: " + str(job[0]) + " " + currency + " - " + str(job[2]) + " " + currency + " in " + str(
                konfidenzgrad) + "% of cases")
            print("Expected Time needed: " + str(job[4]) + " hours")
            print("Best Starting Time: " + str(job[3]))
            print()
    else:
        print("Jobs #1")
        print("Best Instance: " + str(single_costs[6]) + "in Region: " + str(single_costs[5]))
        print("Expected Price: " + str(single_costs[1]) + " " + currency)
        print("Price Range: " + str(single_costs[0]) + " " + currency + " - " + str(single_costs[2]) + " " + currency + " in " + str(
            konfidenzgrad) + "% of cases")
        print("Expected Time needed: " + str(single_costs[4]) + " hours")
        print("Best Starting Time: " + str(single_costs[3]))

def generate_output(total_cost, single_costs, konfidenzgrad, multiple, provider):
    """
        Builds and solves a linear model picking exactly ONE combination of:
          (r1, r2, i, s, p)

        Where:
          - storage is in region r1 (with instance i, parallel factor p),
          - transfer is (r1->r2),
          - compute is in region r2 (with instance i, parallel factor p),
            starting time s.

        The total cost = storage_cost_map[r1, i, p]
                       + transfer_cost_map[r1, r2]
                       + compute_cost_map[r2, i, s, p][0][1]
        Exactly one tuple is chosen (x=1), to minimize total cost.
    """
    if provider == "Azure":
        currency = "€"
    else:
        currency = "$"

    print("Total Costs for " + provider + " :")
    print("Expected Costs: " + str(total_cost[1]) + " " + currency)
    print("Price Range: " + str(total_cost[0]) + " " + currency + " - " + str(total_cost[2]) + currency + " in " + str(
        konfidenzgrad) + "% of cases")
    print("Expected Time needed: " + str(total_cost[3]) + " hours")
    print()
    if multiple:
        for index, job in enumerate(single_costs):
            print("Jobs #" + str(index))
            print("Best Instance: " + str(job[6]) + " in Region: " + str(job[5]))
            print("Expected Price: " + str(job[1]) + " " + currency)
            print("Price Range: " + str(job[0]) + " " + currency + " - " + str(job[2]) + " " + currency + " in " + str(
                konfidenzgrad) + "% of cases")
            print("Expected Time needed: " + str(job[4]) + " hours")
            print("Best Starting Time: " + str(job[3]))
            print()
    else:
        print("Jobs #1")
        print("Best Instance: " + str(single_costs[6]) + "in Region: " + str(single_costs[5]))
        print("Expected Price: " + str(single_costs[1]) + " " + currency)
        print("Price Range: " + str(single_costs[0]) + " " + currency + " - " + str(
            single_costs[2]) + " " + currency + " in " + str(
            konfidenzgrad) + "% of cases")
        print("Expected Time needed: " + str(single_costs[4]) + " hours")
        print("Best Starting Time: " + str(single_costs[3]))

def build_prefix_arrays(costs_per_hour):
    """
    Builds and solves a linear model picking exactly ONE combination of:
      (r1, r2, i, s, p)

    Where:
      - storage is in region r1 (with instance i, parallel factor p),
      - transfer is (r1->r2),
      - compute is in region r2 (with instance i, parallel factor p),
        starting time s.

    The total cost = storage_cost_map[r1, i, p]
                   + transfer_cost_map[r1, r2]
                   + compute_cost_map[r2, i, s, p][0][1]
    Exactly one tuple is chosen (x=1), to minimize total cost.

    Additionally, this method builds prefix sums for a 48-hour extended window of the 24-hour cost array.
    Returns:
      extended_costs, prefix_min, prefix_mean, prefix_max
    """
    extended_costs = costs_per_hour + costs_per_hour

    # Prepare prefix arrays
    prefix_min = [0] * (48 + 1)
    prefix_mean = [0] * (48 + 1)
    prefix_max = [0] * (48 + 1)

    for i in range(48):
        prefix_min[i + 1] = prefix_min[i] + extended_costs[i][0]
        prefix_mean[i + 1] = prefix_mean[i] + extended_costs[i][1]
        prefix_max[i + 1] = prefix_max[i] + extended_costs[i][2]

    return extended_costs, prefix_min, prefix_mean, prefix_max