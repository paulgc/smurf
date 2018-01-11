
from smurf.execute_rf.predicate import Predicate
from smurf.execute_rf.rule import Rule
from smurf.execute_rf.rule_set import RuleSet
from smurf.utils.generic_helper import COMP_OP_MAP

def extract_pos_rules_from_tree(tree, feature_table, start_rule_id, start_predicate_id):
    '''
    Function used to extract positive rules from a tree in random forest
    Given a tree from the random forest and features, this function extracts all the positive rules
    as a set of predicates into a ruleset.

    Args:
    tree (Tree) : A tree from the learnt random forest
    feature_table (DataFrame): A dataframe of all the features extracted. Eg: Jaccard, edit distance
    start_rule_id (int): Starting rule id
    start_predicate_id (int): Starting predicate id

    Returns:
    ruleset: Set of positive rules from the tree
    '''

    feature_names = list(feature_table.index)
    # Get the left, right trees and the threshold from the tree
    left = tree.tree_.children_left
    right = tree.tree_.children_right
    threshold = tree.tree_.threshold

    # Get the features from the tree
    features = [feature_names[i] for i in tree.tree_.feature]
    value = tree.tree_.value

    rule_set = RuleSet()

    def traverse(node, left, right, features, threshold, depth, cache, start_rule_id, curr_predicate_id):
        if node == -1:
            return
        if threshold[node] != -2:
            # node is not a leaf node
            feat_row = feature_table.ix[features[node]]
            p = Predicate(features[node],
                          feat_row['sim_measure_type'],
                          feat_row['tokenizer_type'],
                          feat_row['sim_function'],
                          feat_row['tokenizer'], '<=', threshold[node], feat_row['cost'])
            p.set_name(features[node]+' <= '+str(threshold[node]))
            curr_predicate_id += 1
            cache.insert(depth, p)
            traverse(left[node], left, right, features, threshold, depth+1, cache, start_rule_id, curr_predicate_id)
            prev_pred = cache.pop(depth)
            feat_row = feature_table.ix[features[node]]
            p = Predicate(features[node],
                          feat_row['sim_measure_type'],
                          feat_row['tokenizer_type'],
                          feat_row['sim_function'],
                          feat_row['tokenizer'], '>', threshold[node], feat_row['cost'])
            p.set_name(features[node]+' > '+str(threshold[node]))
            curr_predicate_id += 1
            cache.insert(depth, p)
            traverse(right[node], left, right, features, threshold, depth+1, cache, start_rule_id, curr_predicate_id)
            prev_pred = cache.pop(depth)
        else:
            # node is a leaf node
            if value[node][0][0] <= value[node][0][1]:
                r = Rule(cache[0:depth])
                r.set_name('r'+str(start_rule_id + len(rule_set.rules)+1))
                rule_set.add_rule(r)

    traverse(0, left, right, features, threshold, 0, [], start_rule_id, start_predicate_id)
    return rule_set

def extract_pos_rules_from_rf(rf, feature_table):
    '''
    Function used to extract positive rules from each tree of the random forest
    Given a random forest and features, this function extracts all the positive rules
    as a set of predicates into a list of ruleset.

    Args:
    rf (Random Forest) : A random forest tree learnt from input data
    feature_table (DataFrame): A dataframe of all the features extracted. Eg: Jaccard, edit distance

    Returns:
    list of ruleset: Set of positive rules from the tree
    '''

    rule_sets = []
    rule_id, predicate_id, tree_id = 1, 1, 1
    for dt in rf.estimators_:
        rs = extract_pos_rules_from_tree(dt, feature_table, rule_id, predicate_id)
        rs.set_name('t'+str(tree_id))
        tree_id += 1
        rule_id += len(rs.rules)
        predicate_id += sum(map(lambda r: len(r.predicates), rs.rules))
        rule_sets.append(rs)
    return rule_sets
