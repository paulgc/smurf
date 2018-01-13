

from smurf.utils.extract_rules import extract_pos_rules_from_rf

def execute_rf(ltable, rtable, 
               l_key_attr, r_key_attr, 
               l_join_attr, r_join_attr,
               rf, feature_table,
               allow_empty=True, allow_missing=False,
               l_out_attrs=None, r_out_attrs=None,
               l_out_prefix='l_', r_out_prefix='r_'):

    rule_sets = extract_pos_rules_from_rf(rf, feature_table)
    num_trees = len(rule_sets)

    blocking_set, matching_set = get_trees_for_blocking(rule_sets, feature_table)

    cand_set = execute_rules_for_blocking()

    output = execute_rules_for_matching()

