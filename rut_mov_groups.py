import pandas as pd
import itertools
from group_helpers import create_movement_group, get_movements_with_rut_associated_invoices
from preprocessing import get_preprocessed_invoices_and_movements
from params import MAX_GROUP_LEN, MAX_GROUP_DATE_DIFF

def group_movements(movements):
    mov_groups = []
    for _, movs in movements.groupby(['rut', 'counterparty_rut']):
        group = movs.sort_values(by="mov_date", ascending=True)
        groups = get_group_subgroups(group)
        mov_groups.extend(map(create_movement_group, groups))
    return pd.DataFrame(mov_groups)

def get_group_subgroups(movs):
    seen_keys = set()
    unique_groups = []
    max_group_len = min(MAX_GROUP_LEN, len(movs))
    movs = movs.to_dict('records')
    for i in range(max(1, len(movs) - max_group_len + 1)):
        window_len = min(MAX_GROUP_LEN, len(movs) - i)
        window = movs[i:i + window_len]
        for length in range(2, window_len + 1):
            for comb in itertools.combinations(window, length):
                key = tuple(sorted(m['mov_id'] for m in comb))
                if key not in seen_keys:
                    seen_keys.add(key)
                    min_date = min(map(lambda x: x['mov_date'], comb))
                    max_date = max(map(lambda x: x['mov_date'], comb))
                    if (max_date - min_date).days <= MAX_GROUP_DATE_DIFF:
                        unique_groups.append(list(comb))
    return unique_groups

if __name__ == "__main__":
    invoices, movements = get_preprocessed_invoices_and_movements()
    movements = get_movements_with_rut_associated_invoices(invoices, movements)
    mov_groups = group_movements(movements)
