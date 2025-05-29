import pandas as pd
import itertools
from group_helpers import create_invoice_group
import params

def get_invoice_groups(invoices):
    inv_groups = []
    for _, invs in invoices.groupby(['rut', 'counterparty_rut']):
        group = invs.sort_values(by="inv_date", ascending=True)
        groups = get_group_subgroups(group)
        inv_groups.extend(map(create_invoice_group, groups))
    return pd.DataFrame(inv_groups)

def get_group_subgroups(invs):
    seen_keys = set()
    unique_groups = []
    max_group_len = min(params.MAX_GROUP_LEN, len(invs))
    invs = invs.to_dict('records')
    for i in range(max(1, len(invs) - max_group_len + 1)):
        window_len = min(params.MAX_GROUP_LEN, len(invs) - i)
        window = invs[i:i + window_len]
        for length in range(2, window_len + 1):
            # if (window[length-1]['inv_date'] - window[0]['inv_date']).days > params.MAX_GROUP_DATE_DIFF:
            #     continue
            for comb in itertools.combinations(window, length):
                key = tuple(sorted(m['inv_number'] for m in comb))
                if key not in seen_keys:
                    seen_keys.add(key)
                    min_date = min(map(lambda x: x['inv_date'], comb))
                    max_date = max(map(lambda x: x['inv_date'], comb))
                    if (max_date - min_date).days <= params.MAX_GROUP_DATE_DIFF:
                        unique_groups.append(list(comb))
    return unique_groups
