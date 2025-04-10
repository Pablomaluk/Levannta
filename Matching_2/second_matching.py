import pandas as pd
import numpy as np
import datetime as dt
import recordlinkage
from recordlinkage.preprocessing import clean
from rapidfuzz import fuzz
import re
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, '..'))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from Matching_1.first_matching import get_current_matches_and_pending_invoices_and_movements
pd.set_option('display.max_columns', None)

def get_matches_in_date_range(matches):
    return matches[(matches['mov_date'] >= matches['inv_date'] - dt.timedelta(days=14)) &
                   (matches['mov_date'] <= matches['inv_date'] + dt.timedelta(days=90))]

previous_matches, invoices, movements = get_current_matches_and_pending_invoices_and_movements()
invoices = invoices[invoices['counterparty_rut'] != '555555555']
#print(invoices['counterparty_rut'].value_counts().head(10))
#print(movements['counterparty_rut'].value_counts().head(10))
#print(invoices['rut'].value_counts(), movements['rut'].value_counts())

#Relative difference
def compare_amount_difference(invoices, movements):
    links = pd.merge(invoices, movements, on=['rut','counterparty_rut'])
    links = get_matches_in_date_range(links)
    links['rel_amount_diff'] = \
        abs(links['inv_amount'] - links['mov_amount'])/links['inv_amount']
    links['amount_similarity'] = gaussian_similarity(links['rel_amount_diff'])
    return links

def gaussian_similarity(series):
    scale = 0.05  # ~5% diferencia
    return np.exp(-(series / scale) ** 2)

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    #invoices = invoices[invoices['rut'].isin([763653773])]
    #movements = movements[movements['rut'].isin([763653773])]
    useful_columns = ['inv_amount', 'mov_amount', 'inv_date', 'mov_date']
    gauss_df = compare_amount_difference(invoices, movements)
    gauss_df = gauss_df.sort_values(by='amount_similarity', ascending=False)
    gauss_df = gauss_df[gauss_df['amount_similarity'] < 0.9]
    print(gauss_df[useful_columns + ['rel_amount_diff', 'amount_similarity']].head(50))
    # len_gauss = len(gauss_df)
    # len_sim = len(gauss_df[gauss_df['amount_similarity'] >= 0.1])
    # print(len_sim, len_gauss, f"{100*len_sim/len_gauss}%")
    
    pass