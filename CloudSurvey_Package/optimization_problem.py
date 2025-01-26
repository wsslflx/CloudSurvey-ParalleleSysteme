import pulp

def optimize(
    compute_cost_map,
    storage_cost_map,
    transfer_cost_map
):
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
        # as well as all (r2, i, s) in compute_cost_map that match r2 and i
        #  the same i in both storage and compute.
        # the same p in both storage and compute

        for (r1_s, i_s, p_s) in storage_cost_map.keys():
            if r1_s != r1:
                continue  # mismatch

            # matching (r1, i)
            storage_cost = storage_cost_map[(r1_s, i_s, p_s)]

            for (r2_c, i_c, s_c, p_c) in compute_cost_map.keys():
                if (r2_c == r2) and (i_c == i_s) and (p_c == p_s):
                    # match on region r2 and instance i
                    compute_cost = compute_cost_map[(r2_c, i_c, s_c, p_c)]

                    total_cost = storage_cost + (transfer_c * p_s) + compute_cost[0][1] #use mean cost for compute_cost

                    quintuple = (r1, r2, i_s, s_c, p_s)
                    feasible_keys.append(quintuple)
                    cost_map_combo[quintuple] = total_cost

    # ----------------------------------------------------------------
    # 2) Create decision variables for each feasible quadruple
    x_var = {
        (r1, r2, i, s, p): pulp.LpVariable(
            f"x_{r1}_{r2}_{i}_{s}_p{p}",
            lowBound=0,
            upBound=1,
            cat=pulp.LpBinary
        )
        for (r1, r2, i, s, p) in feasible_keys
    }

    # ----------------------------------------------------------------
    # 3) Constraints
    # Exactly one combination chosen
    model += (
        pulp.lpSum(x_var[q] for q in feasible_keys) == 1,
        "PickExactlyOneChain"
    )

    # ----------------------------------------------------------------
    # 4) Objective: sum of x_var 
    # * combined cost
    model += pulp.lpSum([
        x_var[(r1, r2, i, s, p)] * cost_map_combo[(r1, r2, i, s, p)]
        for (r1, r2, i, s, p) in feasible_keys
    ]), "MinimizeTotalCost"

    # ----------------------------------------------------------------
    # 5) Solve
    if not feasible_keys:
        print("No feasible keys")
        print(feasible_keys)

    model.solve(pulp.PULP_CBC_CMD(msg=0))

    if pulp.LpStatus[model.status] != 'Optimal':
        raise RuntimeError("Optimization did not converge to an optimal solution.")

    return model, x_var



