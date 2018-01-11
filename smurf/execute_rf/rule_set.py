

class RuleSet:
    def __init__(self, rules=None):
        if rules is None:
            self.rules = []
        else:
            self.rules = rules

    def set_name(self, name):
        self.name = name

    def add_rule(self, rule):
        self.rules.append(rule)

    def set_cost(self, cost):
        self.cost = cost

    def set_coverage(self, coverage):
        self.coverage = coverage
        return True
