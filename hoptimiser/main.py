import numpy as np
import pandas as pd
from component_inputs_reader import read_component_data, populate_combinations
from variable_price_azure import Analysis

if __name__ == '__main__':

    input_file_name_components = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\inputs\component_inputs.xlsx'
    tank_df, electrolyser_df, data_years = read_component_data(input_file_name_components)

    combinations_with_stack_replacements = populate_combinations(tank_df, electrolyser_df, input_file_name_components)


    final_combinations = []
    lcoh2 = []
    for combination in combinations_with_stack_replacements:

        analysis = Analysis(input_combination=str(combination))
        lcoh2_this_combination = round(analysis.run(),1)
        lcoh2.append(lcoh2_this_combination)

        final_combinations.append(combination)


    final_results = pd.DataFrame()
    final_results['combination'] = final_combinations
    final_results['lcoh2'] = lcoh2

    final_results.to_csv('../results/final_results.csv')
