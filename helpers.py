import os
import pandas as pd

def read_stage_dfs(num):
    matches = pd.read_csv(os.path.join(f"Matching_{num}", 'Matches.csv'))
    invoices = pd.read_csv(os.path.join(f"Matching_{num}", 'Pending Invoices.csv'))
    movements = pd.read_csv(os.path.join(f"Matching_{num}", 'Pending Movements.csv'))
    invoices['inv_date'] = pd.to_datetime(invoices['inv_date']).dt.date
    movements['mov_date'] = pd.to_datetime(movements['mov_date']).dt.date
    return matches, invoices, movements

def save_stage_dfs(matches, pending_invoices, pending_movements, num):
    matches.to_csv(os.path.join(f"Matching_{num}", 'Matches.csv'), index=False)
    pending_invoices.to_csv(os.path.join(f"Matching_{num}", 'Pending Invoices.csv'), index=False)
    pending_movements.to_csv(os.path.join(f"Matching_{num}", 'Pending Movements.csv'), index=False)

def print_matches_percentage_per_rut(matches, pending_invoices, pending_movements):
    ruts = matches['rut'].unique().tolist()
    for rut in ruts:
        inv_matches = matches[matches['rut'] == rut]['inv_number'].nunique()
        mov_matches = matches[matches['rut'] == rut]['mov_id'].nunique()
        invs_pending = len(pending_invoices[pending_invoices['rut'] == rut])
        movs_pending = len(pending_movements[pending_movements['rut'] == rut])
        print(f"RUT: {rut}; Invs assigned: {round(100*inv_matches/(inv_matches + invs_pending), 2)}%; Movs assigned: {round(100*mov_matches/(mov_matches + movs_pending), 2)}%")
    print('')