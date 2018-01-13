
from smurf.utils.generic_helper import COMP_OP_MAP


class Predicate:
    def __init__(self, feat_name, sim_measure_type, tokenizer_type, sim_function,
                 tokenizer, comp_op, threshold, cost):
        self.feat_name = feat_name
        self.sim_measure_type = sim_measure_type
        self.tokenizer_type = tokenizer_type
        self.sim_function = sim_function
        self.tokenizer = tokenizer
        self.comp_op = comp_op
        self.threshold = threshold
        self.comp_fn = COMP_OP_MAP[self.comp_op]
        self.cost = cost

    def set_name(self, name):
        self.name = name

    def set_coverage(self, coverage):
        self.coverage = coverage
        return True

    def is_valid_join_predicate(self):
        if self.sim_measure_type in ['JACCARD',
                                     'COSINE',
                                     'DICE',
                                     'OVERLAP',
                                     'OVERLAP_COEFFICIENT']:

            return self.comp_op in ['>', '>=', '=']
        elif self.sim_measure_type == SIMILARITY_MEASURES.edit_distance:
                return self.comp_op in ['<', '<=', '=']
        return False
