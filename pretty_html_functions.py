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

    complete_rule = {'id': (rule['id']), 'rule_in_html': Markup(pretty_html), 'rule_base': rule_base_dict,
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


def one_instance_html(instance, columns, sensitive_columns):
    pretty_html = ''
    i = 0
    for column in sensitive_columns:
        pretty_html += "<b>" + column + " = " + instance[column] + "</b>"
        i += 1
        if i == len(sensitive_columns):
            pretty_html += "<br> <br>"
        else:
            pretty_html += ", "

    i = 0
    for column in columns:
        if i == (len(columns)-len(sensitive_columns)):
            pretty_html += "<br>"
        if (column not in sensitive_columns):
            pretty_html += "<b>" + column + "</b> = <i>" + str(instance[column]) + "</i> <br>"
        i+= 1
    return Markup(pretty_html)


def decision_ratio_information(number_of_instances, pos_decision_ratio):
    percentage_ratio = pos_decision_ratio * 100
    return f"Out of {number_of_instances} similar instances, {percentage_ratio:.2f}% have a different decision than the selected instance"


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

