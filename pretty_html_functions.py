import json
import ast
from markupsafe import Markup

def dicts_to_html(rule_base_dict, rule_consequence_dict, rule, max_rule_length=3):
    pretty_html = ''
    for key, value in rule_base_dict.items():
        pretty_html += "<b>" + key + "</b> = " + value + "<br>"

    line_breaks_to_add = max_rule_length - len(rule_base_dict) + 1
    for i in range(line_breaks_to_add):
        pretty_html += '<br>'

    rule_consequence_key = list(rule_consequence_dict.keys())[0]
    pretty_html += "<b>" + rule_consequence_key + "</b> = " + rule_consequence_dict[rule_consequence_key]

    pretty_support = f"{rule['support']:.2f}"
    pretty_confidence = f"{rule['confidence']:.2f}"
    pretty_slift = f"{rule['slift']:.2f}"

    complete_rule = {'id': int(rule['id']), 'rule_in_html': Markup(pretty_html), 'rule_base': rule_base_dict,
                     'rule_conclusion': rule_consequence_dict, 'support': pretty_support,
                     'confidence': pretty_confidence, 'slift': pretty_slift}
    return complete_rule



def rule_row_to_html(rule):
    rule_base_dict = ast.literal_eval(rule['rule_base'])
    rule_consequence_dict = ast.literal_eval(rule['rule_conclusion'])

    return dicts_to_html(rule_base_dict, rule_consequence_dict, rule)


def rule_dict_to_html(rule):
    rule_base_dict = json.loads(rule['rule_base'])
    rule_consequence_dict = json.loads(rule['rule_conclusion'])

    return dicts_to_html(rule_base_dict, rule_consequence_dict, rule)


def one_instance_html(instance, columns, numerical_columns, sensitive_columns):
    pretty_html = ''
    i = 0
    for column in sensitive_columns:
        pretty_html += "<b>" + column + " = " + instance[column] + "</b>"
        i += 1
        if i == len(sensitive_columns):
            pretty_html += "<br>"
        else:
            pretty_html += ", "


    for column in columns:
        if (column not in numerical_columns) & (column not in sensitive_columns):
            pretty_html += "<b>" + column + "</b> = <i>" + instance[column] + "</i> <br>"
    return Markup(pretty_html)


def protected_info_html(instance):
    pretty_html = instance['race'].capitalize()
    if instance['sex'] == 'Female':
        pretty_html += " Woman"
    else:
        pretty_html += " Man"
    return pretty_html


def protected_itemset_info_to_html(pd_itemset):
    pd_itemset_dict = ast.literal_eval(pd_itemset)

    pretty_html = ""
    if 'race' in pd_itemset_dict:
        pretty_html += pd_itemset_dict['race'] + " "

    if 'sex' in pd_itemset_dict:
        if pd_itemset_dict['sex'] == 'Female':
            pretty_html += "Women"
        else:
            pretty_html += "Men"
    else:
        pretty_html += " people"
    return pretty_html


def decision_ratio_information(number_of_instances, pos_decision_ratio):
    percentage_ratio = pos_decision_ratio * 100
    return f"Out of {number_of_instances} similar instances, {percentage_ratio:.2f}% have a high income"


def disc_pattern_to_one_line_html(pattern):
    rule_base_dict = json.loads(pattern['rule_base'])
    pd_itemset_dict = json.loads(pattern['pd_itemset'])
    rule_consequence_dict = json.loads(pattern['rule_conclusion'])
    complete_rule_base = {**rule_base_dict, **pd_itemset_dict}

    one_line_html = ""

    index = 0
    for key, value in complete_rule_base.items():
        one_line_html += "<b> " + key + " </b> = " + value
        if index<(len(complete_rule_base)-1):
            one_line_html += ", "
        index += 1

    one_line_html += "<br>"

    rule_consequence_key = list(rule_consequence_dict.keys())[0]
    one_line_html += "&rightarrow; <b>" + rule_consequence_key + "</b> = " + rule_consequence_dict[rule_consequence_key]

    return Markup(one_line_html)

