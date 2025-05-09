import pandas as pd
import json
from copy import deepcopy

def query_builder_multiple_filters(relevant_table, relevant_columns, column_value_filter_dict):
    value_list = []

    query = 'select ' + relevant_columns + ' from ' + relevant_table + ' where'
    i = 0
    for column_filter in column_value_filter_dict:
        if i > 0:
            query += ' AND'
        query += " " + column_filter + " = (?)"
        i += 1
        value_list.append(column_value_filter_dict[column_filter])

    return query, value_list

#
# def query_builder_multiple_filters(column_value_filter_dict):
#     value_list = []
#
#     query = 'select * from test_data where'
#     i = 0
#     for column_filter in column_value_filter_dict:
#         if i > 0:
#             query += ' AND'
#         query += " " + column_filter + " = (?)"
#         i += 1
#         value_list.append(column_value_filter_dict[column_filter])
#
#     return query, value_list


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


def get_indices_covered_by_pattern(pattern, dataset_df):
    rule_base_without_pd_items_dict = json.loads(pattern['rule_base'])
    prot_itemset_dict = json.loads(pattern['pd_itemset'])

    rule_base_dict = {**prot_itemset_dict, **rule_base_without_pd_items_dict}
    rule_consequence_dict = json.loads(pattern['rule_conclusion'])
    instances_covered = get_instances_covered_by_rule_base_and_consequence(rule_base_dict, rule_consequence_dict, dataset_df)

    indices_of_instances_covered = list(instances_covered.index.values)
    return indices_of_instances_covered

def get_instances_not_falling_under_rule_base(data, rule_base):
    print(rule_base)
    data_falling_under_rule_base = deepcopy(data)
    for key in rule_base:
        data_falling_under_rule_base = data_falling_under_rule_base[data[key] == rule_base[key]]

    data_not_falling_under_rule_base = data[~data.index.isin(data_falling_under_rule_base.index)]

    print(data_not_falling_under_rule_base)
    return data_not_falling_under_rule_base


def get_data_from_itemset_not_falling_under_rules(data, disc_patterns):
    print(disc_patterns)
    print(data)
    rule_bases = disc_patterns["rule_base"]
    print(rule_bases)
    #
    for _,rule_base in rule_bases.items():
        rule_base_dict = json.loads(rule_base)
        data = get_instances_not_falling_under_rule_base(data, rule_base_dict)

    return data
#

def get_relevant_columns_in_pattern(pattern):
    rule_base_without_pd_items_dict = json.loads(pattern['rule_base'])
    prot_itemset_dict = json.loads(pattern['pd_itemset'])
    rule_consequence_dict = json.loads(pattern['rule_conclusion'])

    rule_base_dict = {**prot_itemset_dict, **rule_base_without_pd_items_dict, **rule_consequence_dict}
    return rule_base_dict.keys()


def get_relevant_columns_in_pattern_without_consequence(pattern):
    rule_base_without_pd_items_dict = json.loads(pattern['rule_base'])
    prot_itemset_dict = json.loads(pattern['pd_itemset'])

    rule_base_dict = {**prot_itemset_dict, **rule_base_without_pd_items_dict}
    return rule_base_dict.keys()

def decide_keep_reject_fair_data(row, t_f_unc):
    if row['pred_probability'] < t_f_unc:
        return "Reject"
    else:
        return "Keep"

def decide_keep_reject_flip_rule_covered_data(row, t_sit_test, t_unf_unc, t_f_unc):
    #unfair case
    if row['disc_score'] > t_sit_test:
        if row['pred_probability'] < t_unf_unc:
            return "Flip"
        else:
            return "Reject"
    #fair case
    else:
        if row['pred_probability'] < t_f_unc:
            return "Reject"
        else:
            return "Keep"
