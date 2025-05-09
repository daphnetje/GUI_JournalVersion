from flask import Flask, request, jsonify, url_for, redirect, session, render_template, g
from flask_caching import Cache
import sqlite3
from database_helper_functions import query_builder_multiple_filters, get_indices_covered_by_pattern, get_instances_covered_by_rule_base, get_instances_covered_by_rule_base_and_consequence, get_relevant_columns_in_pattern, get_relevant_columns_in_pattern_without_consequence, decide_keep_reject_flip_rule_covered_data, get_data_from_itemset_not_falling_under_rules, decide_keep_reject_fair_data
from pretty_html_functions import rule_dict_to_html, disc_pattern_to_one_line_html, rule_row_to_html, one_instance_html, protected_info_html, decision_ratio_information, protected_itemset_info_to_html
from situation_testing import number_of_positive_decisions, run_situation_testing
import ast

import pandas as pd

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
app.config['DEBUG'] = True
app.config['SECRET_KEY'] = 'Tf42Cq7ZtH5TsoZfiYXrpSkN7xUGzWaV'

dataset_dict = {'age': ["Younger than 30", "30 - 39", "40-49", "50-59", "Older than 60"],
                'marital_status': ['Never married', 'Married', 'Seperated', 'Widowed'],
                'workinghours' : ["Less than 20", "20 to 39", "40 to 49", "More than 50"],
                'education' : ['Middle School', 'High School', 'Higher Ed.', 'Bachelors', 'Masters', 'Doctorate'],
                'workclass' : ['self-employed', 'private', 'government'],
                'occupation': ['Exec-managerial', 'Machine-op-inspct', 'Prof-specialty', 'Other-service', 'Adm-clerical', 'Transport-moving', 'Sales', 'Craft-repair', 'Farming-fishing', 'Tech-support', 'Other', 'Handlers-cleaners'],
                'race' : ['White', 'Black'],
                'sex' : ["Female", "Male"],
                'income' : ['<=50K', '>50K']}
numerical_columns = ['age_num', 'hours_per_week_num', 'education_num']
all_columns = ['age', 'marital_status', 'workinghours', 'education', 'workclass', 'occupation', 'race', 'sex', 'income']
all_columns_with_id = ['id', 'age', 'marital_status', 'workinghours', 'education', 'workclass', 'occupation', 'race', 'sex', 'income']
all_columns_with_id_and_pred_proba = ['id', 'age', 'marital_status', 'workinghours', 'education', 'workclass', 'occupation', 'race', 'sex', 'income', 'pred_probability']
situation_testing_threshold = 0.2 #all disc_scores with sit_test < 0.2 are not seen as unfair
uncertainty_threshold_unfair_data = 0.58 #if pred. proba is smaller than flip
uncertainty_threshold_fair_data = 0.53 #if pred. proba is smaller than reject

dataset_loaded = False

def connect_db():
    sql = sqlite3.connect('bias_detection.db')
    #ensures that rows are displayed as python dictionaries instead of tuples
    sql.row_factory = sqlite3.Row
    return sql

def get_db():
    if not hasattr(g, 'sqlite3'):
        g.sqlite_db = connect_db()
    return g.sqlite_db


@app.teardown_appcontext
def close_db(error):
    if hasattr(g, 'sqlite_db'):
        g.sqlite_db.close()

#
# @app.route('/', methods = ['GET', 'POST'])
# def index():
#     if request.method == 'POST':
#         selected_column_filters = {}
#
#         # Get the number of dynamically generated select fields
#         num_select_fields = len([key for key in request.form.keys() if key.startswith('dependent_select_')])
#
#         # Iterate through the select fields and insert data into the database
#         for i in range(1, num_select_fields + 1):
#             selected_column_name = request.form.get(f'main_select_{i}')
#             selected_value = request.form.get(f'dependent_select_{i}')
#             selected_column_filters[selected_column_name] = selected_value
#
#         db = get_db()
#         query_to_filter, values_to_filter = query_builder_multiple_filters(relevant_table='test_data',relevant_columns='*', column_value_filter_dict=selected_column_filters)
#         adult_dataset_cursor = db.execute(query_to_filter, values_to_filter)
#         results = adult_dataset_cursor.fetchall()
#         print(len(results))
#         adult_dataset_cursor.close()
#         return render_template('home.html', column_names=dataset_dict.keys(), numerical_cols = numerical_columns, results=results, number_of_results = len(results))
#
#     db = get_db()
#     adult_dataset_cursor = db.execute('select * from test_data')
#     results = adult_dataset_cursor.fetchall()
#     adult_dataset_cursor.close()
#     return render_template('home.html', column_names = dataset_dict.keys(),numerical_cols = numerical_columns, results = results, number_of_results = len(results))

@app.route('/', methods = ['GET', 'POST'])
def inspect_demographic_groups():
    db = get_db()

    disc_patterns_cursor = db.execute("SELECT id, pd_itemset, rule_base, rule_conclusion, support, confidence, slift, p_value_slift FROM discriminatory_patterns")
    all_disc_patterns = disc_patterns_cursor.fetchall()
    disc_patterns_cursor.close()
    disc_patterns_df = pd.DataFrame.from_records(data=all_disc_patterns, columns=["id", "pd_itemset", "rule_base", "rule_conclusion", "support", "confidence", "slift", "p_value_slift"])
    unique_pd_itemsets = disc_patterns_df["pd_itemset"].unique()

    adult_test_dataset_cursor = db.execute(
        'select id, age, marital_status, workinghours, education, workclass, occupation, race, sex, income from test_data')
    adult_dataset_test_results = adult_test_dataset_cursor.fetchall()
    adult_test_dataset_cursor.close()

    # put the validation dataset (that already has been fetched in previous query) in df format
    test_dataset = pd.DataFrame.from_records(data=adult_dataset_test_results, index='id', columns=all_columns_with_id)

    pd_itemset_rule_dict = {}
    pd_itemset_html_dict = {}
    pd_itemset_pos_ratio = {}
    pd_itemset_n = {}

    for pd_itemset in unique_pd_itemsets:
        pd_itemset_in_html = protected_itemset_info_to_html(pd_itemset)
        rules_for_pd_itemset = disc_patterns_df[disc_patterns_df['pd_itemset'] == pd_itemset]
        rules_in_pretty_html = rules_for_pd_itemset.apply(rule_row_to_html, axis=1)

        pd_itemset_dict = ast.literal_eval(pd_itemset)
        n_instances_in_pd_itemset = len(get_instances_covered_by_rule_base(pd_itemset_dict, test_dataset))
        n_instances_in_pd_itemset_pos_decision = len(get_instances_covered_by_rule_base_and_consequence(rule_base=pd_itemset_dict, rule_consequence={"income" : "high"}, data=test_dataset))
        pos_ratio_pd_itemset = n_instances_in_pd_itemset_pos_decision/n_instances_in_pd_itemset

        pd_itemset_rule_dict[pd_itemset] = rules_in_pretty_html
        pd_itemset_html_dict[pd_itemset] = pd_itemset_in_html
        pd_itemset_pos_ratio[pd_itemset] = f"{pos_ratio_pd_itemset:.2f}"
        pd_itemset_n[pd_itemset] = n_instances_in_pd_itemset

    print(pd_itemset_html_dict)

    return render_template("inspect_demographic_groups.html", patterns=pd_itemset_rule_dict, pd_itemset_html_dict=pd_itemset_html_dict, pd_itemset_n = pd_itemset_n, pd_itemset_pos_ratio=pd_itemset_pos_ratio)


#@app.route('/inspect_patterns/<pd_itemset>/', defaults = {'pattern_id': -99999}, methods = ['GET'])
#@app.route('/inspect_patterns/<pd_itemset>/<pattern_id>', methods = ['GET', 'POST'])
@app.route('/inspect_patterns/<pd_itemset>/', methods = ['GET', 'POST'])
def inspect_patterns(pd_itemset):
    db = get_db()
    pd_itemset_in_html = protected_itemset_info_to_html(pd_itemset)

    # Get pattern_id from query parameters (instead of URL path)
    pattern_id = request.args.get("pattern_id", -99999, type=int)
    pos_ratio_pd_itemset = request.args.get("pos_ratio_pd_itemset")
    n_pd_itemset = request.args.get("n_pd_itemset")

    #get all discriminatory patterns
    disc_patterns_cursor = db.execute('select * from discriminatory_patterns where pd_itemset = ?', [pd_itemset])
    disc_patterns_results = disc_patterns_cursor.fetchall()
    disc_patterns_df = pd.DataFrame.from_records(data=disc_patterns_results, columns=["id", "pd_itemset", "rule_base", "rule_conclusion", "support", "confidence", "slift", "p_value_slift"])
    disc_decision_rules = [rule_dict_to_html(result) for result in disc_patterns_results]

    pd_itemset_dict = ast.literal_eval(pd_itemset)
    query_to_filter, values_to_filter = query_builder_multiple_filters(relevant_table='test_data', relevant_columns = 'id, age, marital_status, workinghours, education, workclass, occupation, race, sex, income, pred_probability', column_value_filter_dict=pd_itemset_dict)
    adult_test_dataset_cursor = db.execute(query_to_filter, values_to_filter)
    adult_dataset_test_results = adult_test_dataset_cursor.fetchall()
    adult_test_dataset_cursor.close()

    adult_val_dataset_cursor = db.execute(
        'select id, age, marital_status, workinghours, education, workclass, occupation, race, sex, income from validation_data')
    adult_dataset_val_results = adult_val_dataset_cursor.fetchall()
    adult_val_dataset_cursor.close()

    val_dataset = pd.DataFrame.from_records(data=adult_dataset_val_results, index='id', columns=all_columns_with_id)
    full_test_dataset = pd.DataFrame.from_records(data=adult_dataset_test_results, index='id', columns=all_columns_with_id_and_pred_proba)
    test_data_pred_proba = full_test_dataset['pred_probability']
    test_dataset = full_test_dataset.loc[:, full_test_dataset.columns != 'pred_probability']

    fair_data = get_data_from_itemset_not_falling_under_rules(data=full_test_dataset, disc_patterns=disc_patterns_df)
    fair_data["decision"] = fair_data.apply(decide_keep_reject_fair_data, axis=1, args=(uncertainty_threshold_fair_data, ))
    number_to_reject = len(fair_data[fair_data['decision'] == 'Reject'])
    number_to_keep = len(fair_data[fair_data['decision'] == 'Keep'])

    fair_data_sorted_by_pred_proba = fair_data.sort_values(by='pred_probability', ascending=True)

    relevant_indices = []
    number_of_affected_instances = 0
    number_to_flip = 0
    sorted_data_by_disc_score = pd.DataFrame([])
    disc_scores_list = []
    column_names_in_pattern = []

    if (pattern_id != -99999):
        selected_pattern_cursor = db.execute('select * from discriminatory_patterns where id = ?', [pattern_id])
        selected_pattern = selected_pattern_cursor.fetchone()
        selected_pattern_cursor.close()

        column_names_in_pattern = get_relevant_columns_in_pattern(selected_pattern)
        relevant_indices = get_indices_covered_by_pattern(selected_pattern, test_dataset)
        relevant_data = test_dataset.loc[relevant_indices]
        relevant_pred_proba = test_data_pred_proba.loc[relevant_indices]

        number_of_affected_instances = len(relevant_indices)

        disc_scores_of_relevant_indices, nearest_reference_neighbors_df, nearest_non_reference_neighbors_df = run_situation_testing(selected_pattern, relevant_data, val_dataset)
        disc_scores_list = disc_scores_of_relevant_indices.tolist()
        relevant_data['disc_score'] = disc_scores_of_relevant_indices
        relevant_data['pred_probability'] = relevant_pred_proba
        relevant_data['decision'] = relevant_data.apply(decide_keep_reject_flip_rule_covered_data, axis=1, args=(situation_testing_threshold, uncertainty_threshold_unfair_data, uncertainty_threshold_fair_data))
        number_to_flip = len(relevant_data[relevant_data['decision'] == 'Flip'])
        number_to_reject = len(relevant_data[relevant_data['decision'] == 'Reject'])
        number_to_keep = len(relevant_data[relevant_data['decision'] == 'Keep'])


        print(relevant_data['decision'])
        #sort according to disc_score
        sorted_data_by_disc_score = relevant_data.sort_values(by=['pred_probability', 'disc_score'], ascending=[True, False])

    if request.method == 'POST':
        selected_index = int(request.form['selected_index'])
        disc_score_of_index = round(disc_scores_of_relevant_indices.loc[selected_index], 2)
        prediction_probability_of_index = round(relevant_pred_proba.loc[selected_index], 2)

        nearest_reference_neighbors_of_instance = nearest_reference_neighbors_df.loc[selected_index].tolist()
        nearest_non_reference_neighbors_of_instance =nearest_non_reference_neighbors_df.loc[selected_index].tolist()
        closest_non_ref_indices_string_format = ','.join(str(i) for i in nearest_non_reference_neighbors_of_instance)
        closest_ref_group_indices_string_format = ','.join(str(i) for i in nearest_reference_neighbors_of_instance)

        return redirect(url_for("inspect_nearest_neighbours", index = selected_index, disc_score = disc_score_of_index, pred_proba = prediction_probability_of_index, closest_non_ref=closest_non_ref_indices_string_format, closest_ref=closest_ref_group_indices_string_format, selected_pattern_id=pattern_id))

    return render_template('inspect_pd_itemset.html', pd_itemset=pd_itemset, pd_itemset_in_html = pd_itemset_in_html, column_names = all_columns_with_id,
                           n_pd_itemset = n_pd_itemset, pos_ratio_pd_itemset = pos_ratio_pd_itemset, fair_data = fair_data_sorted_by_pred_proba,
                           dataset_results = full_test_dataset, rules = disc_decision_rules, active_pattern_id = int(pattern_id),
                           column_names_in_pattern = column_names_in_pattern,
                           indices_to_highlight = relevant_indices, disc_scores_of_indices = disc_scores_list,
                           sorted_by_disc_score_data_to_highlight = sorted_data_by_disc_score,
                           number_of_affected_instances = number_of_affected_instances, sit_test_t = situation_testing_threshold,
                           fair_unc_t = uncertainty_threshold_fair_data, unfair_unc_t = uncertainty_threshold_unfair_data,
                           number_to_reject = number_to_reject, number_to_flip = number_to_flip, number_to_keep = number_to_keep)



@app.route('/inspect_nearest_neighbours/<index>/<disc_score>/<pred_proba>/<closest_non_ref>/<closest_ref>/<selected_pattern_id>', methods=['GET', 'POST'])
def inspect_nearest_neighbours(index, disc_score, pred_proba, closest_non_ref, closest_ref, selected_pattern_id):
    db = get_db()

    selected_instance_cursor = db.execute("SELECT age, marital_status, workinghours, education, workclass, occupation, race, sex, income from test_data WHERE id=?",[index])
    selected_instance = selected_instance_cursor.fetchone()
    selected_instance_html = one_instance_html(selected_instance, columns=dataset_dict.keys(),
                                               numerical_columns=numerical_columns, sensitive_columns=['race', 'sex'])
    prot_info_html = protected_info_html(selected_instance)

    selected_pattern_cursor = db.execute('select * from discriminatory_patterns where id = ?', [selected_pattern_id])
    selected_pattern = selected_pattern_cursor.fetchone()
    selected_pattern_cursor.close()
    selected_pattern_html = disc_pattern_to_one_line_html(selected_pattern)
    column_names_in_pattern = get_relevant_columns_in_pattern_without_consequence(selected_pattern)

    if (closest_non_ref != 'None'):
        closest_prot_list = list(closest_non_ref.split(","))
        #+1 is not so nice here, it's because database ids start counting from 1 rather than 0. should change this in database!!
        closest_prot_placeholder = ','.join(['?'] * len(closest_prot_list))

        adult_dataset_cursor_prot = db.execute(f"SELECT age, marital_status, workinghours, education, workclass, occupation, race, sex, income FROM validation_data WHERE id IN ({closest_prot_placeholder})", closest_prot_list)
        dataset_closest_non_ref_results = adult_dataset_cursor_prot.fetchall()
        dataset_closest_non_ref_results_df = pd.DataFrame.from_records(data=dataset_closest_non_ref_results, columns=all_columns)
        pos_decision_ratio_non_ref = number_of_positive_decisions(dataset_closest_non_ref_results_df['income']) / len(dataset_closest_non_ref_results)
        pos_decision_ratio_non_ref_html = decision_ratio_information(len(dataset_closest_non_ref_results_df), pos_decision_ratio_non_ref)
    else:
        pos_decision_ratio_non_ref_html = ""
        dataset_closest_prot_results = "None"

    if (closest_ref!= 'None'):
        closest_ref_list = list(closest_ref.split(","))
        closest_ref_placeholder = ','.join(['?'] * len(closest_ref_list))

        adult_dataset_cursor_ref = db.execute(f"SELECT age, marital_status, workinghours, education, workclass, occupation, race, sex, income FROM validation_data WHERE id IN ({closest_ref_placeholder})",
                                               closest_ref_list)
        dataset_closest_ref_results = adult_dataset_cursor_ref.fetchall()
        dataset_closest_ref_results_df = pd.DataFrame.from_records(data=dataset_closest_ref_results,
                                                                    columns=all_columns)
        pos_decision_ratio_ref = number_of_positive_decisions(dataset_closest_ref_results_df['income']) / len(
            dataset_closest_ref_results)
        pos_decision_ratio_ref_html = decision_ratio_information(len(dataset_closest_ref_results_df),
                                                                  pos_decision_ratio_ref)
    else:
        pos_decision_ratio_ref_html = ""
        dataset_closest_ref_results = "None"

    return render_template('inspect_nearest_neighbours.html', selected_index=index, prot_info = prot_info_html, selected_instance_html = selected_instance_html,
                           selected_pattern_html = selected_pattern_html, column_names_in_pattern = column_names_in_pattern,
                           selected_instance=selected_instance, disc_score = disc_score, pred_proba = pred_proba, closest_prot_results = dataset_closest_non_ref_results,
                           closest_ref_results = dataset_closest_ref_results, pos_ratio_prot = pos_decision_ratio_non_ref_html, pos_ratio_ref = pos_decision_ratio_ref_html,
                           column_names = dataset_dict.keys())

if __name__=='__main__':
    app.run(debug = True)

