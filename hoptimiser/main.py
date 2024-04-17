import numpy as np
import pandas as pd
from component_inputs_reader import read_component_data, populate_combinations
from variable_price import variable_price_runner

input_file_name_components = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\examples\component_inputs.xlsx'
tank_df, electrolyser_df, data_years = read_component_data(input_file_name_components)

combinations = populate_combinations(tank_df, electrolyser_df)

combinations_with_stack_replacements = []#combinations.copy()

for combination in combinations:

    for stack_replacement_period in range(7, 8):
        for cutoff in range (stack_replacement_period + 1, 12):
            n = 1
            new_combo = combination.copy()
            endpoint = min(len(data_years) - 1, cutoff)
            while stack_replacement_period * n <= endpoint:
                new_combo.append(stack_replacement_period * n)
                n += 1

            if new_combo not in combinations_with_stack_replacements:
                combinations_with_stack_replacements.append(new_combo)

final_combinations = []
lcoh2 = []

for combination in combinations_with_stack_replacements:

    result = variable_price_runner(tank_df, electrolyser_df, data_years, input_combination=combination)

    final_combinations.append(combination)
    lcoh2.append(result)

final_results = pd.DataFrame()
final_results['combination'] = final_combinations
final_results['lcoh2'] = lcoh2

final_results.to_csv('results/final_results.csv')
