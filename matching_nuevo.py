import pandas as pd
import numpy as np
import recordlinkage
from recordlinkage.preprocessing import clean
from rapidfuzz import fuzz
import re

from preprocessing import get_preprocessed_invoices_and_movements


if __name__ == '__main__':
    invoices, movements = get_preprocessed_invoices_and_movements()
    indexer = recordlinkage.Index()
    indexer.block(['rut', 'counterparty_rut'])
    candidate_links = indexer.index(invoices, movements)
    compare = recordlinkage.Compare()
    compare.exact('inv_amount', 'mov_amount', label='amount_match')
    features = compare.compute(candidate_links, invoices, movements)
    print(features[features['amount_match'] == 1])