import pandas as pd
from scipy.spatial.distance import cdist
import json
from database_helper_functions import get_instances_covered_by_rule_base_and_consequence, get_instances_covered_by_rule_base



def number_of_positive_decisions(decision_vector):
    value_counts = decision_vector.value_counts()
    if 'high' in value_counts.keys():
        return value_counts['high']
    else:
        return 0

def positive_decision_ratio(data, neighbours_indices):
    decision_info_of_neighbours = data.loc[neighbours_indices, 'income']
    positive_decision_count = (decision_info_of_neighbours == 'high').sum()
    return positive_decision_count/ len(neighbours_indices)  # Compute the ratio

def run_situation_testing(selected_pattern, test_instances_covered_by_rule, val_dataset):
    #similar instances are found within the space of other instances that are covered by rule base (excluding the prot.
    #information from rule base)
    rule_base_without_protected_group = json.loads(selected_pattern['rule_base'])
    prot_group_of_pattern = json.loads(selected_pattern['pd_itemset'])

    #TODO: consider if I really want to only search among instances for which the rule base is covered
    val_instances_covered_by_rule = get_instances_covered_by_rule_base(rule_base_without_protected_group, val_dataset)

    #extract group of other protected instances and reference group
    val_reference_group = val_instances_covered_by_rule[(val_instances_covered_by_rule['sex'] == 'Male') & (val_instances_covered_by_rule['race'] == 'White')]

    #STILL HAVE TO DECIDE HERE WHAT WE WILL TAKE AS OTHER PROT INSTANCES (maybe best to look within specific intersectional
    # spaces like, white women, black women, black men, etc)
    val_non_reference_group = val_instances_covered_by_rule[(val_instances_covered_by_rule['sex'] != 'Male') | (val_instances_covered_by_rule['race'] != 'White')]

    distances_to_non_reference_group = cdist(test_instances_covered_by_rule, val_non_reference_group,
                                             metric=distance_function_adult_dataset)
    distance_df_to_non_reference = pd.DataFrame(distances_to_non_reference_group, columns=val_non_reference_group.index,
                                                       index=test_instances_covered_by_rule.index)
    # Find the k nearest neighbors of the non_reference_group for each index in the dataset
    nearest_non_reference_neighbors = distance_df_to_non_reference.apply(lambda row: row.nsmallest(5).index.tolist(), axis=1)

    nearest_non_reference_neighbors_df = pd.DataFrame(nearest_non_reference_neighbors.tolist(), index=test_instances_covered_by_rule.index,
                                               columns=[f'Neighbor_{i + 1}' for i in range(5)])


    distances_to_reference_group = cdist(test_instances_covered_by_rule, val_reference_group,
                                         metric=distance_function_adult_dataset)
    distance_df_to_reference = pd.DataFrame(distances_to_reference_group, columns=val_reference_group.index,
                                                   index=test_instances_covered_by_rule.index)
    # Find the k nearest neighbors of the reference group for each index in the dataset
    nearest_reference_neighbors = distance_df_to_reference.apply(lambda row: row.nsmallest(5).index.tolist(), axis=1)
    nearest_reference_neighbors_df = pd.DataFrame(nearest_reference_neighbors.tolist(), index=test_instances_covered_by_rule.index,
                                                             columns=[f'Neighbor_{i + 1}' for i in range(5)])

    pos_ratio_non_reference_neighbours = nearest_non_reference_neighbors_df.apply(
        lambda row: positive_decision_ratio(val_non_reference_group, row), axis=1)
    pos_ratio_reference_neighbours = nearest_reference_neighbors_df.apply(
        lambda row: positive_decision_ratio(val_reference_group, row), axis=1)

    disc_scores = pos_ratio_reference_neighbours - pos_ratio_non_reference_neighbours


    # disc_scores = []
    # i = 0
    # #for index in range(len(relevant_instances_numerical_format)):
    # for index in relevant_indices:
    #     distance_row_non_reference= distances_to_non_reference_group_df.loc[index]
    #     #closest_indices_non_reference = distance_row_non_reference[distance_row_non_reference <= 0.8].index.tolist()
    #     sorted = distance_row_non_reference.argsort()
    #     sorted_indices_non_reference = distance_row_non_reference.iloc[sorted].index
    #     closest_indices_non_reference = sorted_indices_non_reference[:5]
    #     decision_info_closest_non_ref_neighbours = val_non_reference_group.loc[closest_indices_non_reference]['income']
    #
    #     distance_row_reference = distances_to_reference_group_df.loc[index]
    #     #closest_indices_reference = distance_row_reference[distance_row_reference <= 0.8].index.tolist()
    #     sorted = distance_row_reference.argsort()
    #     sorted_indices_unprotected = distance_row_reference.iloc[sorted].index
    #     closest_indices_reference = sorted_indices_unprotected[:5]
    #     decision_info_closest_reference_neighbours = val_reference_group.loc[closest_indices_reference]['income']
    #
    #     number_of_similar_prots = len(decision_info_closest_non_ref_neighbours)
    #     number_of_similar_refs = len(decision_info_closest_reference_neighbours)
    #
    #     if number_of_similar_prots > 0:
    #         ratio_pos_labels_non_reference = number_of_positive_decisions(decision_info_closest_non_ref_neighbours) / len(decision_info_closest_non_ref_neighbours)
    #
    #     if number_of_similar_refs > 0:
    #         ratio_pos_labels_reference = number_of_positive_decisions(decision_info_closest_reference_neighbours) / len(decision_info_closest_reference_neighbours)
    #
    #     if (number_of_similar_refs == 0) | (number_of_similar_prots == 0) :
    #         disc_score = -999
    #     else:
    #         disc_score = ratio_pos_labels_reference - ratio_pos_labels_non_reference
    #
    #     disc_scores.append(disc_score)
    #
    # disc_scores = pd.Series(disc_scores, index=relevant_indices)
    return disc_scores, nearest_reference_neighbors_df, nearest_non_reference_neighbors_df


#order of features: ['age_num', 'marital status', 'workinghours_num', 'education_num', 'workclass', 'occupation', 'race', 'sex', 'income']]
def distance_function_adult_dataset(x1, x2):
    age_dict = {"Younger than 25": 1, "25-29": 2, "30-39": 3, "40-49": 4, "50-59": 5, "60-69": 6, "Older than 70": 7}
    age_diff = abs(age_dict[x1[0]] - age_dict[x2[0]]) / 6

    if x1[1] == x2[1]:
        marital_status_diff = 0
    else:
        marital_status_diff = 0.4

    workinghours_dict = {"Less than 20": 1, "20-39": 2, "40-49": 4, "More than 50": 5}
    workinghours_diff = abs(workinghours_dict[x1[2]] - workinghours_dict[x2[2]]) / 3

    education_dict = {"No Elementary School":1, "Elementary School":2, "Middle School":3,
                            "Started High School, No Diploma":4, "High School or GED Diploma":5,
                            "Started College, No Diploma":6, "Associate Degree":7, "Bachelor Degree":8,
                            "Master or other Degree Beyond Bachelor":9, "Doctorate Degree":10}
    education_diff = abs(education_dict[x1[3]] - education_dict[x2[3]]) / 9


    if x1[4] == x2[4]:
        workclass_diff = 0
    else:
        workclass_diff = 0.5

    if x1[5] == x2[5]:
        occupation_diff = 0
    else:
        occupation_diff = 0.7

    return age_diff + marital_status_diff + education_diff + workinghours_diff + workclass_diff + occupation_diff


# def run_situation_testing(selected_pattern, prot_group_of_pattern, relevant_indices, dataset, cache):
#     distance_matrix = cache.get("distance_matrix")
#     if (distance_matrix is not None):
#         print("we've found the matrix")
#
#     if (distance_matrix is None):
#         print("In if")
#         initializing_parameters_for_cache(dataset, cache)
#         distance_matrix = cache.get("distance_matrix")
#
#     relevant_distance_rows = distance_matrix.iloc[relevant_indices]
#
#     protected_instances = cache.get("protected_instances")
#     unprotected_instances = cache.get("unprotected_instances")
#
#     distances_to_protected_indices = distance_matrix[protected_instances.index]
#     distances_to_unprotected_indices = distance_matrix[unprotected_instances.index]
#
#     disc_scores = []
#     i = 0
#     for index in range(len(relevant_distance_rows)):
#
#         distance_row_protected = distances_to_protected_indices.iloc[index]
#         sorted = distance_row_protected.argsort()
#         sorted_indices_protected = distance_row_protected.iloc[sorted].index
#         closest_indices_protected = sorted_indices_protected[:5]
#         decision_info_closest_prot_neighbours = protected_instances.loc[closest_indices_protected]['income']
#
#         distance_row_unprotected = distances_to_unprotected_indices.iloc[index]
#         sorted = distance_row_unprotected.argsort()
#         sorted_indices_unprotected = distance_row_unprotected.iloc[sorted].index
#         closest_indices_unprotected = sorted_indices_unprotected[:5]
#         decision_info_closest_unprot_neighbours = unprotected_instances.loc[closest_indices_unprotected]['income']
#
#         ratio_pos_labels_prot = number_of_positive_decisions(decision_info_closest_prot_neighbours) / 5
#         ratio_pos_labels_unprot = number_of_positive_decisions(decision_info_closest_unprot_neighbours) / 5
#
#         disc_score = ratio_pos_labels_unprot - ratio_pos_labels_prot
#         disc_scores.append(disc_score)
#
#     return disc_scores, distances_to_protected_indices, distances_to_unprotected_indices
