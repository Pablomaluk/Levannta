import pandas as pd
from pulp import LpProblem, LpMaximize, LpVariable, lpSum, LpBinary
from collections import defaultdict
from params import MAX_GROUP_LEN, SIMILARITY_WEIGHT, SIZE_WEIGHT, DATE_WEIGHT

def assign(matches):
    matches = calculate_scores(matches)
    matches = solve_ilp(matches)
    matches.to_parquet('Results.parquet', index=False)
    return matches


def calculate_scores(matches):
    matches['size_score'] = matches['match_size'].apply(
        lambda x: 1 - ((x-1)/(MAX_GROUP_LEN**2-1))/2
    )
    matches['date_score'] = matches['date_diff'].apply(
        lambda x: 1 - x / 180
    )
    matches['score'] = (matches['amount_similarity'] * SIMILARITY_WEIGHT *
                        matches['size_score'] * SIZE_WEIGHT *
                        matches['date_score'] * DATE_WEIGHT)
    return matches[['inv_group_numbers', 'mov_group_ids', 'score', 'amount_similarity', 'size_score', 'date_score']]

def solve_ilp(matches):
    prob = LpProblem("InvoiceMovementMatching", LpMaximize)
    x = {
        idx: LpVariable(f"x_{idx}", cat=LpBinary)
        for idx in matches.index
    }

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

    prob.solve()

    selected = matches.loc[[idx for idx in matches.index if x[idx].varValue == 1.0]].copy()
    return selected

if __name__ == "__main__":
    matches = pd.read_parquet("Candidates.parquet")
    assign(matches)
