from apyori import apriori
from copy import deepcopy

class Dataset:
    def __init__(self, descriptive_data, decision_attribute, negative_label):
        self.descriptive_data = descriptive_data
        self.decision_attribute = decision_attribute
        self.negative_label = negative_label

        self.give_class_label_info()

    def __str__(self):
        return (self.descriptive_data.head(10).to_string())


    def give_class_label_info(self):
        instances_with_neg_label = self.descriptive_data[self.descriptive_data[self.decision_attribute] == self.negative_label]

        number_instances_with_neg_label = len(instances_with_neg_label)

        print("Ratio of instances with negative label: {0:.2f}".format(number_instances_with_neg_label/len(self.descriptive_data)))



    def extract_class_label_info_for_fraction_of_data(self, extract_dict):
        relevant_data = self.descriptive_data
        for key, value in extract_dict.items():
            relevant_data = relevant_data[relevant_data[key] == value]

        instances_with_neg_label = relevant_data[relevant_data[self.decision_attribute] == self.negative_label]
        number_instances_with_neg_label = len(instances_with_neg_label)

        print("Ratio of instances with negative label: {0:.2f}".format(
            number_instances_with_neg_label / len(relevant_data)))


    def extract_class_label_info_for_all_except_extract_dict(self, do_not_extract_dict):
        non_relevant_data = self.descriptive_data
        for key in do_not_extract_dict.keys():
            non_relevant_data = non_relevant_data[non_relevant_data[key] == do_not_extract_dict[key]]

        index_relevance_boolean_indicators = self.descriptive_data.index.isin(non_relevant_data.index)
        relevant_data = self.descriptive_data[~index_relevance_boolean_indicators]

        instances_with_neg_label = relevant_data[relevant_data[self.decision_attribute] == self.negative_label]
        number_instances_with_neg_label = len(instances_with_neg_label)

        print("Ratio of instances with negative label: {0:.2f}".format(
            number_instances_with_neg_label / len(relevant_data)))


class PD_itemset:
    def __init__(self, dict_notation):
        self.dict_notation = dict_notation
        self.frozenset_notation = self.convert_to_frozenset_notation()

    def __str__(self):
        return str(self.dict_notation)

    def convert_to_frozenset_notation(self):
        initial_set = set()
        for key, item in self.dict_notation.items():
            string_notation = key + " : " + item
            initial_set.add(string_notation)
        return frozenset(initial_set)


class Rule:

    #rule_base and rule_consequence are both dictionaries
    def __init__(self, rule_base, rule_consequence, support, confidence, lift):
        self.rule_base = rule_base
        self.rule_consequence = rule_consequence
        self.support = support
        self.confidence = confidence
        self.lift = lift


    def __str__(self):
        output_string = "Rule: ("
        counter = 1
        for rule_key in self.rule_base.keys():
            output_string += rule_key + " = " + self.rule_base[rule_key]
            if counter != len(self.rule_base):
                output_string += " AND "
            counter += 1
        output_string += ") -> "

        counter = 1
        for rule_key in self.rule_consequence.keys():
            output_string += rule_key + " = " + self.rule_consequence[rule_key]
            if counter != len(self.rule_consequence):
                output_string += " AND "
            counter += 1

        print("Support: " + str(self.support))
        print("Confidence: " + str(self.confidence))
        return output_string



def convert_frozenset_rule_format_to_dict_format(frozentset_rule_representation):
    rule_as_dict = {}
    #rule is a frozenset, where each item follows the following format 'key : value'
    #each itemstring needs to be added to the dictionary as one key, value pair
    for rule_item in frozentset_rule_representation:
        #rule_item is string
        splitted_rule = rule_item.split(" : ")
        rule_as_dict[splitted_rule[0]] = splitted_rule[1]

    return rule_as_dict


def initialize_rule(rule_base_frozenset, rule_consequence_frozenset, support, confidence, lift):
    rule_base_dict = convert_frozenset_rule_format_to_dict_format(rule_base_frozenset)
    rule_consequence_dict = convert_frozenset_rule_format_to_dict_format(rule_consequence_frozenset)
    rule = Rule(rule_base_dict, rule_consequence_dict, support, confidence, lift)
    return rule

def get_instances_covered_by_rule_base_and_consequence(rule_base, rule_consequence, data):
    relevant_data = data
    for key in rule_base.keys():
        relevant_data = relevant_data[relevant_data[key] == rule_base[key]]

    for key in rule_consequence.keys():
        relevant_data = relevant_data[relevant_data[key] == rule_consequence[key]]

    return relevant_data

def get_instances_covered_by_rule_base(rule_base, data):
    relevant_data = data
    for key in rule_base.keys():
        relevant_data = relevant_data[relevant_data[key] == rule_base[key]]
    return relevant_data


def convert_to_apriori_format(dataframe):
    list_of_dicts_format = dataframe.to_dict('record')
    list_of_sets = []
    for dictionary in list_of_dicts_format:
        one_entry = set()
        for key, value in dictionary.items():
            one_entry.add(key + " : " + str(value))
        list_of_sets.append(one_entry)
    return list_of_sets


def longest_fitting_protected_group_in_rule(rule, protected_itemsets):
    size_of_biggest_fitting_pd_itemset = 0
    #on position 0 there's always the empty pd itemset
    biggest_fitting_pd_itemset = protected_itemsets[0]
    for protected_itemset in protected_itemsets:
        if len(protected_itemset.frozenset_notation.intersection(rule)) == len(protected_itemset.frozenset_notation) :
            if len(protected_itemset.frozenset_notation) > size_of_biggest_fitting_pd_itemset:
                biggest_fitting_pd_itemset = protected_itemset
                size_of_biggest_fitting_pd_itemset = len(protected_itemset.frozenset_notation)
    return biggest_fitting_pd_itemset


def rule_contains_protected(rule, protected_itemsets):
    for protected_itemset in protected_itemsets:
        if not rule.isdisjoint(protected_itemset.frozenset_notation):
            return True

    return False



def extract_potentially_discriminating_rules(all_rules, class_items, protected_itemsets):
    rules_connected_to_class = {}
    rules_connected_to_protected_itemsets = {}

    for protected_itemset in protected_itemsets:
        rules_connected_to_class[protected_itemset] = []
        rules_connected_to_protected_itemsets[protected_itemset] = []

    counter = 0
    for rule in all_rules:
        counter+=1
        #covering the case that we're dealing with a rule that contains decision attribute
        if not rule.items.isdisjoint(class_items):
            #make sure that we save the ordering of the rule, where the consequence ONLY consists of the decision attribute
            for ordering in rule.ordered_statistics:
                rule_base = ordering.items_base
                rule_consequence = ordering.items_add
                if (not rule_consequence.isdisjoint(class_items)) & (len(rule_consequence) == 1):
                    longest_fitting_protected_group = longest_fitting_protected_group_in_rule(rule_base,
                                                                                              protected_itemsets)
                    myRule = initialize_rule(rule_base, rule_consequence, support=rule.support,
                                             confidence=ordering.confidence, lift=ordering.lift)

                    entry = {'rule': myRule, 'support': rule.support, 'confidence': ordering.confidence, 'lift': ordering.lift}
                    rules_connected_to_class[longest_fitting_protected_group].append(entry)

        #needed to find patterns of indirect discrimination, and have rules of the form X -> Sensitive_attribute
        #covering the case that the rule consists of a protected itemset
        if rule_contains_protected(rule.items, protected_itemsets):
            for ordering in rule.ordered_statistics:
                rule_base = ordering.items_base
                rule_consequence = ordering.items_add
                longest_fitting_protected_group = longest_fitting_protected_group_in_rule(rule_consequence,
                                                                                          protected_itemsets)

                #we want to have the rule that has the longest possible protected itemset in consequence
                if rule_consequence==longest_fitting_protected_group.frozenset_notation:
                    myRule = initialize_rule(rule_base, rule_consequence, support = rule.support, confidence = ordering.confidence, lift = ordering.lift)
                    entry = {'rule': myRule, 'support': rule.support, 'confidence': ordering.confidence, 'lift': ordering.lift}
                    rules_connected_to_protected_itemsets[longest_fitting_protected_group].append(entry)

    return rules_connected_to_class, rules_connected_to_protected_itemsets



#rule come in this format {'rule_base': {'sex': 'Male'}, 'rule_consequence': {'income': '<=50K'}, 'support': 0.46460489542704464, 'confidence': 0.6942634235888022, 'lift': 0.9144786138946193}
def calculate_slift_measure(rule_dict, data, protected_itemset):
    rule = rule_dict['rule']

    pd_itemset_dict_notation = protected_itemset.dict_notation
    pd_itemset_frozenset_notation = protected_itemset.frozenset_notation

    #check if class rule contains protected itemset. If not than the DCI score will be 0
    if pd_itemset_frozenset_notation==frozenset():
        return 0

    confidence_of_original_rule = rule_dict['confidence']
    rule_base_without_protected_itemset = deepcopy(rule.rule_base)
    for key in pd_itemset_dict_notation.keys():
        rule_base_without_protected_itemset.pop(key, None)
    comparison_group = {"sex": "Male", "race": "White"}
    comparison_group.update(rule_base_without_protected_itemset)
    number_instances_covered_with_comparison_group_base = len(get_instances_covered_by_rule_base(comparison_group, data))
    complete_comparison_rule = {**comparison_group, **rule.rule_consequence}
    number_of_instances_covered_by_complete_rule_with_comparison_pd = len(
        get_instances_covered_by_rule_base(complete_comparison_rule, data))


    #INSTRUCTIONS TO FUTURE DAPHNE: uncomment this part if you want to calculate the slift of rule by how much confidence
    #changes if negating the sensitive part of the rule (if e.g. sens = Black Women, we end up comparing rule's confidence
    #to all other categories incl. white women, black men)
    # confidence_of_original_rule = rule_dict['confidence']
    # #remove all protected parts from rest of rule base
    # rule_base_without_protected_itemset = deepcopy(rule.rule_base)
    # for key in pd_itemset_dict_notation.keys():
    #     rule_base_without_protected_itemset.pop(key, None)
    # #see how confidence of rule changes when protected-part of rule base is negated
    # number_instances_covered_with_comparison_group_base = len(get_instances_covered_by_rule_with_negation(rule_base_without_protected_itemset, pd_itemset_dict_notation, data))
    # complete_negated_rule = {**rule_base_without_protected_itemset, **rule.rule_consequence}
    # number_of_instances_covered_by_complete_rule_with_comparison_pd = len(get_instances_covered_by_rule_with_negation(complete_negated_rule, pd_itemset_dict_notation, data))

    if number_instances_covered_with_comparison_group_base != 0:
        confidence_comparison_pd = number_of_instances_covered_by_complete_rule_with_comparison_pd / number_instances_covered_with_comparison_group_base
    else:
        return 0

    if confidence_comparison_pd > 0:
        slift = confidence_of_original_rule / confidence_comparison_pd
        return slift
    else:
        return 0



def analyze_slift_of_class_rules(class_rules, complete_dataset):
    negative_label_dict = {complete_dataset.decision_attribute : complete_dataset.negative_label}
    data = complete_dataset.descriptive_data
    rules_sorted_by_slift_per_pd = dict()
    for protected_itemset, rules_connected_to_itemset in class_rules.items():
        rule_slift_dict = dict()
        for rule_dict in rules_connected_to_itemset:
            rule = rule_dict['rule']
            #only calculate dci if rule is connected to negative class label
            if (rule.rule_consequence == negative_label_dict):
                slift_for_rule = calculate_slift_measure(rule_dict, data, protected_itemset)
                rule_slift_dict[rule] = slift_for_rule
        sorted_by_slift = sorted(rule_slift_dict.items(), key=lambda x: x[1], reverse=True)
        rules_sorted_by_slift_per_pd[protected_itemset] = sorted_by_slift
    return rules_sorted_by_slift_per_pd


def find_disc_patterns(dataframe):
    dataset = Dataset(dataframe, decision_attribute="income", negative_label="<=50K")

    formatted_data = convert_to_apriori_format(dataset.descriptive_data)

    association_rules = apriori(transactions=formatted_data, min_support=0.02, min_confidence=0.0,
                                min_lift=0, min_length=2, max_length=5)
    list_of_association_rules = list(association_rules)

    class_items = frozenset(["income : >50K", "income : <=50K"])
    pd_empty = PD_itemset(dict())
    pd_female_and_white = PD_itemset({"sex": "Female", "race": "White"})
    pd_female_and_black = PD_itemset({"sex": "Female", "race": "Black"})
    pd_male_and_black = PD_itemset({"sex": "Male", "race": "Black"})

    pd_itemsets = [pd_empty, pd_female_and_white, pd_female_and_black, pd_male_and_black]

    class_rules, rules_connected_to_prot_itemsets = extract_potentially_discriminating_rules(list_of_association_rules, class_items, pd_itemsets)

    rules_sorted_by_slift_per_pd = analyze_slift_of_class_rules(class_rules, dataset)

    rules_sorted_by_slift_women = rules_sorted_by_slift_per_pd[pd_female_and_white]
    # #
    for (rule, slift) in rules_sorted_by_slift_women:
        print(rule)
        print(slift)

