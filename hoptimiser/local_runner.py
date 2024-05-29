import os
import pandas as pd
from hoptimiser.variable_price_azure import Analysis
from hoptimiser.component_inputs_reader import read_component_data, populate_combinations

class HoptimiserLocalRunner:

    def __init__(self, analysis_name: str, combinations: list):
        self.analysis_name = analysis_name
        self.combinations = combinations

    def run(self):

        for combination in self.combinations:

            analysis = Analysis(
                input_combination=str(combination), run_in_azure=False, reduce_efficiencies=True
            )

            lcoh2_this_combination = round(analysis.run(), 1)

analysis_name = 'test-analysis'

input_file_name_components = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\inputs\component_inputs.xlsx'
tank_df, electrolyser_df, data_years = read_component_data(input_file_name_components)
combinations = populate_combinations(tank_df, electrolyser_df, input_file_name_components)

run_in_azure = False

local_runner = HoptimiserLocalRunner(analysis_name=analysis_name,combinations=combinations)

local_runner.run()

# results = pd.read_csv('local_results.csv')
#
# results['electrolyser_id'] = None
# results['number_of_electrolysers'] = None
# results['electrolyser_manufacturer'] = None
# results['electrolyser_capacity'] = None
# results['tank_id'] = None
# results['number_of_tanks'] = None
# results['tank_manufacturer'] = None
# results['tank_capacity'] = None
# results['stack_replacement_years'] = None
#
# for i in range(0, len(results)):
#
#     combination = results.loc[i, 'combination'][1:-1].split(sep = ',')
#
#     results.loc[i, 'electrolyser_id'] = electrolyser_df.loc[int(combination[0]), 'id']
#     results.loc[i, 'electrolyser_manufacturer'] = electrolyser_df.loc[int(combination[0]), 'Manufacturer']
#     results.loc[i, 'tank_id'] = tank_df.loc[int(combination[2]), 'id']
#     results.loc[i, 'tank_manufacturer'] = tank_df.loc[int(combination[2]), 'Manufacturer']
#     results.loc[i, 'number_of_electrolysers'] = int(combination[1])
#     results.loc[i, 'electrolyser_capacity'] = electrolyser_df.loc[int(combination[0]), 'Capacity (MW)']
#     results.loc[i, 'number_of_tanks'] = int(combination[3])
#     results.loc[i, 'tank_capacity'] = tank_df.loc[int(combination[2]), 'H2 MWh Capacity']
#     if len(combination) > 4:
#         results.loc[i, 'stack_replacement_years'] = str(combination[4:])
#
#     results.to_csv('local_results.csv')
#
