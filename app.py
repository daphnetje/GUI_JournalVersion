from flask import Flask, request, jsonify, url_for, redirect, session, render_template, g
from flask_caching import Cache
import sqlite3
from database_helper_functions import number_of_positive_decisions, query_builder_multiple_filters, get_instances_covered_by_rule_base, get_instances_covered_by_rule_base_and_consequence, get_relevant_columns_in_pattern_without_consequence
from pretty_html_functions import disc_pattern_to_one_line_html, one_instance_html, decision_ratio_information
import ast
from settings import selector_related_columns

import pandas as pd

app = Flask(__name__)
cache = Cache(app, config={'CACHE_TYPE': 'simple'})
app.config['DEBUG'] = True
app.config['SECRET_KEY'] = 'Tf42Cq7ZtH5TsoZfiYXrpSkN7xUGzWaV'


dataset_loaded = False

# Global variables
task = None
decision_attribute = None
positive_label = None
positive_label_dict = None
sensitive_groups = None
relevant_html_dict = None


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

@app.route('/', methods = ['GET', 'POST'])
def choose_decision_task():
    global decision_attribute, sensitive_groups, task, positive_label, positive_label_dict, relevant_html_dict

    options = ["adult", "oulad", "recidivism", "census", "mortgage"]

    if request.method == 'POST':
        selected_task = request.form.get('task')

        if selected_task == 'adult':
            task = "adult"
            decision_attribute = "income"
            positive_label = "high"
            positive_label_dict = {"income": "high"}
            sensitive_groups = ['sex', 'race']
            relevant_html_dict = {"{\"sex\": \"Male\", \"race\": \"White\"}": "White Men", "{\"sex\": \"Male\", \"race\": \"Black\"}": "Black Men",
                                 "{\"sex\": \"Female\", \"race\": \"White\"}": "White Women", "{\"sex\": \"Female\", \"race\": \"Black\"}": "Black Women"}
        elif selected_task == 'oulad':
            task = "oulad"
            decision_attribute = "final_result"
            positive_label = "Pass"
            positive_label_dict = {"final_result": "Pass"}
            sensitive_groups = ['disability']
            relevant_html_dict = {"{\"disability\": \"N\"}": "People Without Disability", "{\"disability\": \"Y\"}": "People With Disability"}

        elif selected_task == 'recidivism':
            task = 'recidivism'
            decision_attribute = 'recidivism'
            positive_label = "no"
            positive_label_dict = {"recidivism": "no"}
            sensitive_groups = ["race"]
            relevant_html_dict = {"{\"race\": \"Caucasian\"}": "Caucasian People", "{\"race\": \"Other\"}": "People with Other Race", "{\"race\": \"African American\"}": "African American People"}

        if selected_task == 'census':
            task = "census"
            decision_attribute = "income"
            positive_label = "high"
            positive_label_dict = {"income": "high"}
            sensitive_groups = ['sex']
            relevant_html_dict = {"{\"sex\": \"Male\"}": "Men",
                                  "{\"sex\": \"Female\"}": "Women"}

        if selected_task == 'mortgage':
            task = "mortgage"
            decision_attribute = "action_taken"
            positive_label = "Approved"
            positive_label_dict = {"action_taken": "Approved"}
            sensitive_groups = ['derived_race']
            relevant_html_dict = {"{\"derived_race\": \"White\"}": "White People",
                                  "{\"derived_race\": \"Black or African American\"}": "Black People"}

        return redirect(url_for('get_overview', task=task))

    return render_template('choose_decision_task.html', options=options)


@app.route('/overview', methods = ['GET', 'POST'])
def get_overview():
    db = get_db()

    disc_patterns_cursor = db.execute("SELECT * FROM " + task+"_discriminatory_patterns")
    # Fetch column names from the cursor description
    column_names = [desc[0] for desc in disc_patterns_cursor.description]
    all_disc_patterns = disc_patterns_cursor.fetchall()
    disc_patterns_cursor.close()
    disc_patterns_df = pd.DataFrame.from_records(data=all_disc_patterns, columns=column_names)
    unique_pd_itemsets = disc_patterns_df["pd_itemset"].unique()

    test_data_cursor = db.execute("SELECT * FROM " + task + "_test_data")
    # Fetch column names from the cursor description
    column_names = [desc[0] for desc in test_data_cursor.description]
    # Fetch all results
    test_data_results = test_data_cursor.fetchall()
    test_data_cursor.close()

    test_data_df = pd.DataFrame.from_records(data=test_data_results, columns=column_names)
    average_pos_ratio = len(test_data_df[test_data_df[decision_attribute] == positive_label]) / len(test_data_df)

    pd_itemset_html_dict = {}
    pd_itemset_org_pos_ratio = {}
    pd_itemset_pos_ratio_after_IFAC = {}
    pd_itemset_n = {}
    pd_itemset_flipped_info = {}
    pd_itemset_reject_info = {}
    pd_itemset_is_discriminated_info = {}

    n_total = 0
    n_total_flipped = 0
    n_total_rejected = 0

    for pd_itemset in unique_pd_itemsets:
        pd_itemset_in_html = relevant_html_dict[pd_itemset]

        pd_itemset_dict = ast.literal_eval(pd_itemset)
        pd_itemset_data = get_instances_covered_by_rule_base(pd_itemset_dict, test_data_df)
        n_instances_in_pd_itemset = len(pd_itemset_data)
        n_total += n_instances_in_pd_itemset
        n_instances_in_pd_itemset_pos_decision = len(get_instances_covered_by_rule_base_and_consequence(rule_base=pd_itemset_dict, rule_consequence=positive_label_dict, data=test_data_df))
        pos_ratio_pd_itemset = n_instances_in_pd_itemset_pos_decision/n_instances_in_pd_itemset
        pd_itemset_is_discriminated = (pos_ratio_pd_itemset < average_pos_ratio)
        pd_itemset_is_discriminated_info[pd_itemset] = pd_itemset_is_discriminated

        pd_itemset_kept_decisions = pd_itemset_data[pd_itemset_data["selector"] == "Keep"]
        n_pos_kept_decisions = len(get_instances_covered_by_rule_base_and_consequence(rule_base=pd_itemset_dict, rule_consequence=positive_label_dict, data=pd_itemset_kept_decisions))
        pd_itemset_flipped_decisions = pd_itemset_data[pd_itemset_data["selector"] == "Fairness-Flip"]
        if pd_itemset_is_discriminated:
             pd_itemset_pos_ratio_after_IFAC[pd_itemset] = (n_pos_kept_decisions + len(pd_itemset_flipped_decisions)) / ((len(pd_itemset_kept_decisions) + len(pd_itemset_flipped_decisions)))
        else:
            pd_itemset_pos_ratio_after_IFAC[pd_itemset] = (n_pos_kept_decisions) / ((len(pd_itemset_kept_decisions) + len(pd_itemset_flipped_decisions)))

        n_instances_with_flipped_decision = len(get_instances_covered_by_rule_base_and_consequence(rule_base=pd_itemset_dict, rule_consequence={"selector" : "Fairness-Flip"}, data=test_data_df))
        n_total_flipped += n_instances_with_flipped_decision
        n_instances_with_unf_reject_decision = len(get_instances_covered_by_rule_base_and_consequence(rule_base=pd_itemset_dict, rule_consequence={"selector" : "Fairness-Reject"}, data=test_data_df))
        n_instances_with_unc_reject_decision = len(get_instances_covered_by_rule_base_and_consequence(rule_base=pd_itemset_dict, rule_consequence={"selector" : "Uncertainty-Reject"}, data=test_data_df))
        n_total_rejected += n_instances_with_unf_reject_decision + n_instances_with_unc_reject_decision

        pd_itemset_html_dict[pd_itemset] = pd_itemset_in_html
        pd_itemset_org_pos_ratio[pd_itemset] = f"{pos_ratio_pd_itemset:.2f}"
        pd_itemset_n[pd_itemset] = n_instances_in_pd_itemset
        pd_itemset_flipped_info[pd_itemset] = n_instances_with_flipped_decision
        pd_itemset_reject_info[pd_itemset] = n_instances_with_unc_reject_decision + n_instances_with_unf_reject_decision

        drop_down_menu_options = ["Original Decisions", "Decisions after IFAC"]
    return render_template("overview.html", n_total=n_total, average_pos_ratio = average_pos_ratio, n_total_flipped = n_total_flipped, n_total_rejected = n_total_rejected,
                           labels = list(pd_itemset_html_dict.values()), org_pos_ratios = list(pd_itemset_org_pos_ratio.values()), new_pos_ratios = list(pd_itemset_pos_ratio_after_IFAC.values()),
                           unique_pd_itemsets = unique_pd_itemsets, options = drop_down_menu_options,
                           pd_itemset_html_dict=pd_itemset_html_dict, pd_itemset_n = pd_itemset_n, pd_itemset_pos_ratio=pd_itemset_org_pos_ratio,
                           pd_itemset_flipped_info = pd_itemset_flipped_info, pd_itemset_reject_info = pd_itemset_reject_info, pd_itemset_is_discriminated_info=pd_itemset_is_discriminated_info)

@app.route('/inspect_one_demographic_group/', methods = ['GET', 'POST'])
def inspect_one_demographic_group():
    pd_itemset = request.args.get("pd_itemset")
    pd_itemset_in_html = request.args.get("pd_itemset_in_html")
    pos_ratio_pd_itemset = request.args.get("pos_ratio_pd_itemset")
    n_pd_itemset = request.args.get("n_pd_itemset")
    n_flipped = request.args.get("n_flipped")
    n_rejected = request.args.get("n_rejected")
    action = request.args.get('action', "")

    db = get_db()
    pd_itemset_dict = ast.literal_eval(pd_itemset)
    query_to_filter, values_to_filter = query_builder_multiple_filters(relevant_table= task +"_test_data",
                                                                       relevant_columns='*',
                                                                       column_value_filter_dict=pd_itemset_dict)
    test_dataset_cursor = db.execute(query_to_filter, values_to_filter)
    # Fetch column names from the cursor description
    column_names = [desc[0] for desc in test_dataset_cursor.description]
    test_results = test_dataset_cursor.fetchall()
    test_dataset_cursor.close()
    test_data_df = pd.DataFrame.from_records(data=test_results, columns=column_names, index='id')

    dataset_columns = [column for column in column_names if column not in selector_related_columns]

    to_display_data = test_data_df[test_data_df["selector"] == action]

    if action == 'Uncertainty-Reject':
        to_display_data = to_display_data.sort_values(by='pred_probability', ascending=True)
        relevant_selector_column = "pred_probability"

    else:
        to_display_data = to_display_data.sort_values(by='GLU_score', ascending=False)
        relevant_selector_column = "GLU_score"

    if request.method == 'POST':
        decision = "Keep" if action=="keep" else "Flip" if action=='Fairness-Flip' else "Fairness-Based Reject" if action=='Fairness-Reject' else "Uncertainty-Based Reject"
        selected_index = int(request.form['selected_index'])
        test_row = test_data_df.loc[selected_index]
        glu_score = test_row['GLU_score']
        sit_disc_score = test_row["sit_test_score"]
        slift = test_row['max_slift']
        pred_proba = test_row["pred_probability"]
        closest_favoured = test_row["closest_favoured"]
        closest_discriminated = test_row["closest_discriminated"]
        relevant_rule = test_row["relevant_rule_id"]
        return redirect(url_for("inspect_one_instance", decision=decision, index = selected_index, glu_score = glu_score, disc_score = sit_disc_score, pred_proba = pred_proba, slift=slift, closest_non_ref=closest_discriminated, closest_ref=closest_favoured, selected_pattern_id=relevant_rule))

    return render_template("inspect_one_group.html", pd_itemset = pd_itemset, pd_itemset_in_html=pd_itemset_in_html,
                           pos_ratio_pd_itemset=pos_ratio_pd_itemset, n_pd_itemset=n_pd_itemset, n_flipped=n_flipped,
                           n_rejected=n_rejected, dataset_columns=dataset_columns, to_display_data = to_display_data,
                           action=action, relevant_selector_column=relevant_selector_column)


@app.route('/inspect_one_instance/<decision>/<index>/<glu_score>/<disc_score>/<pred_proba>/<slift>/<closest_non_ref>/<closest_ref>/<selected_pattern_id>', methods=['GET', 'POST'])
def inspect_one_instance(index, decision, glu_score, disc_score, pred_proba, slift, closest_non_ref, closest_ref, selected_pattern_id):
    db = get_db()
    glu_score = float(glu_score)
    slift = float(slift)
    disc_score = float(disc_score)
    pred_proba = float(pred_proba)

    selected_instance_cursor = db.execute("SELECT * from " + task + "_test_data WHERE id=?",[index])
    column_names = [desc[0] for desc in selected_instance_cursor.description]
    dataset_columns = [column for column in column_names if column not in selector_related_columns]
    print(dataset_columns)
    selected_instance = selected_instance_cursor.fetchone()
    selected_instance_html = one_instance_html(selected_instance, columns=dataset_columns, sensitive_columns=sensitive_groups)

    selected_pattern_cursor = db.execute("select * from " + task+ "_discriminatory_patterns where id = ?", [selected_pattern_id])
    selected_pattern = selected_pattern_cursor.fetchone()
    selected_pattern_cursor.close()
    confidence = selected_pattern['confidence']
    selected_pattern_html = disc_pattern_to_one_line_html(selected_pattern)
    column_names_in_pattern = get_relevant_columns_in_pattern_without_consequence(selected_pattern)

    closest_prot_list = ast.literal_eval(closest_non_ref)
    closest_prot_placeholder = ','.join(['?'] * len(closest_prot_list))
    validation_dataset_cursor_prot = db.execute(f"SELECT * FROM " + task + f"_validation_data WHERE id IN ({closest_prot_placeholder})", closest_prot_list)
    column_names = [desc[0] for desc in validation_dataset_cursor_prot.description]
    dataset_columns = [column for column in column_names if column not in ['id',  'pred_probability']]

    dataset_closest_non_ref_results = validation_dataset_cursor_prot.fetchall()
    dataset_closest_non_ref_results_df = pd.DataFrame.from_records(data=dataset_closest_non_ref_results, columns=column_names)
    dataset_closest_non_ref_results_df = dataset_closest_non_ref_results_df[dataset_columns]

    pos_decision_ratio_non_ref = number_of_positive_decisions(dataset_closest_non_ref_results_df[decision_attribute], positive_decision=positive_label) / len(dataset_closest_non_ref_results)
    pos_decision_ratio_non_ref_html = decision_ratio_information(len(dataset_closest_non_ref_results_df), pos_decision_ratio_non_ref)

    closest_ref_list = ast.literal_eval(closest_ref)
    closest_ref_placeholder = ','.join(['?'] * len(closest_ref_list))

    validation_dataset_cursor_ref = db.execute(f"SELECT * FROM " + task + f"_validation_data WHERE id IN ({closest_ref_placeholder})",
                                               closest_ref_list)
    column_names = [desc[0] for desc in validation_dataset_cursor_ref.description]
    dataset_columns = [column for column in column_names if column not in ['id',  'pred_probability']]

    dataset_closest_ref_results = validation_dataset_cursor_ref.fetchall()
    dataset_closest_ref_results_df = pd.DataFrame.from_records(data=dataset_closest_ref_results,
                                                                columns=column_names)
    dataset_closest_ref_results_df = dataset_closest_ref_results_df[dataset_columns]

    pos_decision_ratio_ref = number_of_positive_decisions(dataset_closest_ref_results_df[decision_attribute], positive_decision=positive_label) / len(
        dataset_closest_ref_results)
    pos_decision_ratio_ref_html = decision_ratio_information(len(dataset_closest_ref_results_df),
                                                              pos_decision_ratio_ref)

    return render_template('inspect_instance.html', decision=decision, selected_index=index, glu_score = glu_score, selected_instance_html = selected_instance_html,
                           sensitive_columns = sensitive_groups, selected_pattern_html = selected_pattern_html, column_names_in_pattern = column_names_in_pattern,
                           selected_instance=selected_instance, slift=slift, confidence=confidence, disc_score = disc_score, pred_proba = pred_proba, closest_prot_results = dataset_closest_non_ref_results,
                           closest_ref_results = dataset_closest_ref_results, pos_ratio_prot = pos_decision_ratio_non_ref_html, pos_ratio_ref = pos_decision_ratio_ref_html,
                           column_names = dataset_columns)


if __name__=='__main__':
    app.run(debug = True)

