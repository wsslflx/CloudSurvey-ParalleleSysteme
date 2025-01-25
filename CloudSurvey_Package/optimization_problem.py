import pulp

def optimize_with_triple_compute(
    compute_cost_map,
    storage_cost_map,
    transfer_cost_map
):
    model = pulp.LpProblem("Unified_Cloud_Optimization", pulp.LpMinimize)

    # ----------------------------------------------------------------
    # Build a combined list of all feasible quadruples (r1, r2, i, s)
    #    - (r1, i) in storage_cost_map
    #    - (r1, r2) in transfer_cost_map
    #    - (r2, i, s) in compute_cost_map

    feasible_keys = []   # list of tuples (r1, r2, i, s)
    cost_map_combo = {}  # dict mapping (r1, r2, i, s) -> total combined cost

    for (r1, r2), transfer_c in transfer_cost_map.items():
        # find all (r1, i) in storage_cost_map that match this r1
        # aswell as all (r2, i, s) in compute_cost_map that match r2 and i
        #  the same i in both storage and compute.

        for (r1_s, i_s) in storage_cost_map.keys():
            if r1_s != r1:
                continue  # mismatch

            # matching (r1, i)
            storage_cost = storage_cost_map[(r1_s, i_s)]
            for (r2_c, i_c, s_c) in compute_cost_map.keys():
                if r2_c == r2 and i_c == i_s:
                    # match on region r2 and instance i
                    compute_cost = compute_cost_map[(r2_c, i_c, s_c)]

                    total_cost = storage_cost + transfer_c + compute_cost[0][1]
                    quadruple = (r1, r2, i_s, s_c)
                    feasible_keys.append(quadruple)
                    cost_map_combo[quadruple] = total_cost

    # ----------------------------------------------------------------
    # 2) Create decision variables for each feasible quadruple
    x_var = {
        (r1, r2, i, s): pulp.LpVariable(
            f"x_{r1}_{r2}_{i}_{s}",
            lowBound=0,
            upBound=1,
            cat=pulp.LpBinary
        )
        for (r1, r2, i, s) in feasible_keys
    }

    # ----------------------------------------------------------------
    # 3) Constraints
    # Exactly one combination chosen
    model += (
        pulp.lpSum(x_var[q] for q in feasible_keys) == 1,
        "PickExactlyOneChain"
    )

    # ----------------------------------------------------------------
    # 4) Objective: sum of x_var * combined cost
    model += pulp.lpSum([
        x_var[(r1, r2, i, s)] * cost_map_combo[(r1, r2, i, s)]
        for (r1, r2, i, s) in feasible_keys
    ]), "MinimizeTotalCost"

    # ----------------------------------------------------------------
    # 5) Solve
    model.solve(pulp.PULP_CBC_CMD(msg=0))

    return model, x_var


