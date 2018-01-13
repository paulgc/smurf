

class Rule:
    def __init__(self, predicates=None):
        if predicates is None:
            self.predicates = []
        else:
            self.predicates = predicates

    def add_predicate(self, predicate):
        self.predicates.append(predicate)

    def set_name(self, name):
        self.name = name

    def set_cost(self, cost):
        self.cost = cost

    def set_coverage(self, coverage):
        self.coverage = coverage
        return True
