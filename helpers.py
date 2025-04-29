import os
import pandas as pd

def get_current_dfs(main_function, path):
    try:
        dfs = read_dfs(path)
    except FileNotFoundError:
        dfs = main_function()
        save_dfs(path, *dfs)
    print(path)
    print_matches_percentage_per_rut(*dfs)
    return dfs

def read_dfs(path):
    invoices = pd.read_csv(os.path.join(path, 'Pending Invoices.csv'))
    movements = pd.read_csv(os.path.join(path, 'Pending Movements.csv'))
    matches = pd.read_csv(os.path.join(path, 'Matches.csv'))
    invoices['inv_date'] = pd.to_datetime(invoices['inv_date']).dt.date
    movements['mov_date'] = pd.to_datetime(movements['mov_date']).dt.date
    return invoices, movements, matches

def save_dfs(path, invoices, movements, matches):
    if not os.path.exists(path):
         os.mkdir(path)
    invoices.to_csv(os.path.join(path, 'Pending Invoices.csv'), index=False)
    movements.to_csv(os.path.join(path, 'Pending Movements.csv'), index=False)
    matches.to_csv(os.path.join(path, 'Matches.csv'), index=False)

def set_exact_match_params(matches):
    matches['rel_amount_diff'] = 0
    matches['amount_similarity'] = 1
    return matches

def print_matches_percentage_per_rut(pending_invoices, pending_movements, matches):
    ruts = matches['rut'].unique().tolist()
    print(f"TOTAL Invoices: {percent(matches['inv_number'].nunique()/(matches['inv_number'].nunique()+len(pending_invoices)))}",
        f"TOTAL Movements: {percent(matches['mov_id'].nunique()/(matches['mov_id'].nunique()+len(pending_movements)))}")
    for rut in ruts:
        inv_matches = matches[matches['rut'] == rut]['inv_number'].nunique()
        mov_matches = matches[matches['rut'] == rut]['mov_id'].nunique()
        invs_pending = len(pending_invoices[pending_invoices['rut'] == rut])
        movs_pending = len(pending_movements[pending_movements['rut'] == rut])
        print(f"RUT: {rut}; Invs assigned: {percent(inv_matches/(inv_matches + invs_pending))}; Movs assigned: {percent(mov_matches/(mov_matches + movs_pending))}")
    print('')

def save_results(dfs):
    invoices, movements, matches = dfs
    ruts_summary = get_excel_summary_per_rut(invoices, movements, matches)
    dates = pd.read_csv("Dates.csv")
    dates['RUT'] = dates['RUT'].astype(str)
    ruts_summary['RUT'] = ruts_summary['RUT'].astype(str)
    print(ruts_summary)
    matches = matches.sort_values(by=['counterparty_rut', 'inv_date', 'mov_date'])
    matches = matches[['rut', 'counterparty_rut', 'inv_amount', 'mov_amount', 'inv_date', 'mov_date', 'inv_number', 'mov_id','mov_description']]
    matches.columns = ['RUT', 'RUT contraparte', 'Monto facturado', 'Monto depositado', 'Fecha factura', 'Fecha depósito', 'Número SII','ID Movimiento','Descripción depósito']
    ruts_summary = pd.merge(ruts_summary, dates, on="RUT")
    with pd.ExcelWriter('Resultados.xlsx') as writer:
        ruts_summary.to_excel(writer, sheet_name="Resumen clientes", index=False)
        matches.to_excel(writer, sheet_name="Matches", index=False)
        invoices.to_excel(writer, sheet_name="Facturas pendientes", index=False)
        movements.to_excel(writer, sheet_name="Movimientos pendientes", index=False)

def get_excel_summary_per_rut(pending_invoices, pending_movements, matches):
    ruts = matches['rut'].unique().tolist()
    ruts_summary = []
    for rut in ruts:
        rut_matches = matches[matches['rut'] == rut]
        rut_pending_invs = pending_invoices[pending_invoices['rut'] == rut]
        rut_pending_movs = pending_movements[pending_movements['rut'] == rut]
        inv_matches = rut_matches['inv_number'].nunique()
        mov_matches = rut_matches[rut_matches['rut'] == rut]['mov_id'].nunique()
        invs_pending = len(rut_pending_invs)
        movs_pending = len(rut_pending_movs)
        total_invs = inv_matches + invs_pending
        total_movs = mov_matches + movs_pending
        prct_matched_invs = round(100*inv_matches/(total_invs), 2)
        prct_matched_movs = round(100*mov_matches/(total_movs), 2)
        matched_inv_amount = sum(rut_matches.groupby('inv_number').first()['inv_amount'])
        matched_mov_amount = sum(rut_matches.groupby('mov_id').first()['mov_amount'])
        pending_inv_amount = sum(rut_pending_invs['inv_amount'])
        pending_mov_amount = sum(rut_pending_movs['mov_amount'])
        total_inv_amount = matched_inv_amount + pending_inv_amount
        total_mov_amount = matched_mov_amount + pending_mov_amount
        prct_inv_amount = round(100*matched_inv_amount/(total_inv_amount), 2)
        prct_mov_amount = round(100*matched_mov_amount/(total_mov_amount), 2)
        ruts_summary.append({
            'RUT': rut,
            'Total facturas':total_invs, 'Total movimientos':total_movs, "Facutras conciliadas %":prct_matched_invs,
            "Movimientos conciliados %": prct_matched_movs, "Monto facturas":total_inv_amount, 
            "Monto movimientos": total_mov_amount, "Monto facturas conciliadas %":prct_inv_amount,
            "Monto movimientos conciliados %":prct_mov_amount
        })
    return pd.DataFrame(ruts_summary).sort_values(by='Facutras conciliadas %', ascending=False)

def add_rut_dash(rut):
    rut = str(rut)
    return f"{rut[:-1]}-{rut[-1]}"

def percent(num):
    return f"{round(100*num, 2)}%"