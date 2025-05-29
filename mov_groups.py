import pandas as pd
import itertools
import datetime as dt
from group_helpers import create_movement_group
import params

def get_movement_groups(movements):
    mov_groups = []
    for _, movs in movements.groupby(['rut', 'counterparty_rut']):
        group = movs.sort_values(by="mov_date", ascending=True)
        groups = get_group_subgroups(group)
        mov_groups.extend(map(create_movement_group, groups))
    return pd.DataFrame(mov_groups)

def get_group_subgroups(movs):
    seen_keys = set()
    unique_groups = []
    max_group_len = min(params.MAX_GROUP_LEN, len(movs))
    movs = movs.to_dict('records')
    for i in range(max(1, len(movs) - max_group_len + 1)):
        window_len = min(params.MAX_GROUP_LEN, len(movs) - i)
        window = movs[i:i + window_len]
        for length in range(2, window_len + 1):
            # if (window[length-1]['mov_date'] - window[0]['mov_date']).days > params.MAX_GROUP_DATE_DIFF:
            #     continue
            for comb in itertools.combinations(window, length):
                key = tuple(sorted(m['mov_id'] for m in comb))
                if key not in seen_keys:
                    seen_keys.add(key)
                    min_date = min(map(lambda x: x['mov_date'], comb))
                    max_date = max(map(lambda x: x['mov_date'], comb))
                    if (max_date - min_date).days <= params.MAX_GROUP_DATE_DIFF:
                        unique_groups.append(list(comb))
    return unique_groups
