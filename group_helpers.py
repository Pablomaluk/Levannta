import numpy as np

def get_invoices_with_rut_associated_movements(invoices, movements):
    return invoices[invoices['counterparty_rut'].isin(movements['counterparty_rut'])]

def get_movements_with_rut_associated_invoices(invoices, movements):
    return movements[movements['counterparty_rut'].isin(invoices['counterparty_rut'])]

def get_invoices_without_rut_associated_movements(invoices, movements):
    return invoices[~invoices['counterparty_rut'].isin(movements['counterparty_rut'])]

def get_movements_without_rut_associated_invoices(invoices, movements):
    return movements[~movements['counterparty_rut'].isin(invoices['counterparty_rut'])]

def create_movement_group(movements):
    group = movements[-1].copy()
    group['mov_amount'] = sum(map(lambda x: x['mov_amount'], movements))
    group['is_mov_group'] = True
    group['mov_group_len'] = len(movements)
    group['first_mov_date'] = movements[0]['mov_date']
    group['last_mov_date'] = movements[-1]['mov_date']
    group['mov_group_ids'] = tuple((map(lambda x: x['mov_id'], movements)))
    group['mov_id'] = np.nan
    group['mov_description'] = np.nan
    return group

def create_invoice_group(invoices):
    group = invoices[0].copy()
    group['inv_amount'] = sum(map(lambda x: x['inv_amount'], invoices))
    group['is_inv_group'] = True
    group['inv_group_len'] = len(invoices)
    group['first_inv_date'] = invoices[0]['inv_date']
    group['last_inv_date'] = invoices[-1]['inv_date']
    group['inv_group_numbers'] = tuple(map(lambda x: x['inv_number'], invoices))
    group['inv_number'] = np.nan
    return group
