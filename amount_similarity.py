import numpy as np
import params

def get_matches_with_similar_amounts(matches):
    matches = calculate_amount_difference(matches)
    matches = matches[matches['rel_amount_diff'] <= params.MAX_REL_AMOUNT_DIFF]
    return calculate_gaussian_similarity(matches)

def calculate_amount_difference(matches):
    matches['rel_amount_diff'] = \
        abs(matches['inv_amount'] - matches['mov_amount'])/matches['inv_amount']
    return matches

def calculate_gaussian_similarity(matches):
    matches = matches.copy()
    matches['amount_similarity'] = np.exp(-(matches['rel_amount_diff'] / params.GAUSSIAN_SIMILARITY_SCALE) ** 2)
    return matches
