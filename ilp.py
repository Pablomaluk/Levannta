import pandas as pd
from pulp import LpProblem, LpMaximize, LpVariable, lpSum, LpBinary, PULP_CBC_CMD
from collections import defaultdict
import params

def optimize(matches):
    matches = calculate_scores(matches)
    matches = solve_ilp(matches)
    return matches

def calculate_scores(matches):
    matches['date_score'] = matches['date_diff'].apply(
        lambda x: 1 - x / 180
    )
    matches['score'] = (matches['amount_similarity'] * matches['date_score'])
    return matches[['inv_group_numbers', 'mov_group_ids', 'score', 'amount_similarity', 'date_score']]

def solve_ilp(matches):
    prob = LpProblem("InvoiceMovementMatching", LpMaximize)
    x = {idx: LpVariable(f"x_{idx}", cat=LpBinary) for idx in matches.index}

    prob += lpSum(matches.loc[idx, 'score'] * x[idx] for idx in matches.index)

    inv_id_usage = defaultdict(list)
    mov_id_usage = defaultdict(list)

    for idx, row in matches.iterrows():
        for inv_id in row['inv_group_numbers']:
            inv_id_usage[inv_id].append(x[idx])
        for mov_id in row['mov_group_ids']:
            mov_id_usage[mov_id].append(x[idx])

    for inv_id, vars in inv_id_usage.items():
        prob += lpSum(vars) <= 1

    for mov_id, vars in mov_id_usage.items():
        prob += lpSum(vars) <= 1

    solver = PULP_CBC_CMD(
        msg=False,
        timeLimit=90,  # seconds
        gapRel=0.01        # stop when within this fraction of optimum
    )
    prob.solve(solver)

    selected = matches.loc[[idx for idx in matches.index if x[idx].varValue == 1.0]].copy()
    return selected
