import pandas as pd
import numpy as np

def read_component_data(input_file_name):

    tank_df = pd.read_excel(input_file_name, sheet_name='Tanks')
    electrolyser_df = pd.read_excel(input_file_name, sheet_name='Electrolysers')
    efficiency_load_factor_df = pd.read_excel(input_file_name, sheet_name='EfficiencyLoadFactorCurves')
    efficiency_degradation_df = pd.read_excel(input_file_name, sheet_name='EfficiencyDegradationCurves')
    stack_capex_learning_df = pd.read_excel(input_file_name, sheet_name='StackReplacementLearningCurves')
    efficiency_learning_df = pd.read_excel(input_file_name, sheet_name='EfficiencyLearningRateCurves')

    tank_df = tank_df[tank_df['Maximum Selectable'] > 0]
    electrolyser_df = electrolyser_df[electrolyser_df['Maximum Selectable'] > 0]

    tank_df = tank_df.reset_index()
    electrolyser_df = electrolyser_df.reset_index()
    electrolyser_df['electrolyser_efficiency'] = np.empty((len(electrolyser_df), 0)).tolist()
    electrolyser_df['electrolyser_efficiency_load_factors'] = np.empty((len(electrolyser_df), 0)).tolist()
    electrolyser_df['efficiency_degradation'] = np.empty((len(electrolyser_df), 0)).tolist()
    electrolyser_df['stack_replacement_capex'] = np.empty((len(electrolyser_df), 0)).tolist()
    electrolyser_df['stack_replacement_capex_year'] = np.empty((len(electrolyser_df), 0)).tolist()
    electrolyser_df['efficiency_learning'] = np.empty((len(electrolyser_df), 0)).tolist()
    electrolyser_df['efficiency_learning_year'] = np.empty((len(electrolyser_df), 0)).tolist()

    for i in range(0, len(electrolyser_df)):
        electrolyser_df.loc[i, 'electrolyser_efficiency'].append(efficiency_load_factor_df[electrolyser_df.loc[i, 'Efficiency Load Factor Curve']].to_list())
        electrolyser_df.loc[i, 'electrolyser_efficiency_load_factors'].append(efficiency_load_factor_df['Load Factor'].to_list())
        electrolyser_df.loc[i, 'efficiency_degradation'].append(efficiency_degradation_df[electrolyser_df.loc[i, 'Efficiency Degradation Curve']].to_list())
        electrolyser_df.loc[i, 'stack_replacement_capex'].append(stack_capex_learning_df[electrolyser_df.loc[i, 'Stack Replacement CAPEX Curve']].to_list())
        electrolyser_df.loc[i, 'stack_replacement_capex_year'].append(stack_capex_learning_df['Year'].to_list())
        electrolyser_df.loc[i, 'efficiency_learning'].append(efficiency_learning_df[electrolyser_df.loc[i, 'Efficiency Learning Rate Curve']].to_list())
        electrolyser_df.loc[i, 'efficiency_learning_year'].append(efficiency_learning_df['Year'].to_list())

    data_years = pd.read_excel(input_file_name, sheet_name='DataYears')

    data_years['combined'] = data_years['DemandYear'] + data_years['PriceYear'].astype(str)

    return tank_df, electrolyser_df, data_years

def populate_combinations(tank_df, electrolyser_df, input_file_name):

    combinations = []
    stack_year_options = pd.read_excel(input_file_name, sheet_name='StackReplacementYears')

    for i in range(0, len(electrolyser_df)):
        for n_electrolysers in range(max(1, electrolyser_df.loc[i, 'Minimum Selectable']), electrolyser_df.loc[i, 'Maximum Selectable']+1):
            for j in range(0, len(tank_df)):
                for n_tanks in range(max(1, tank_df.loc[j, 'Minimum Selectable']), tank_df.loc[j, 'Maximum Selectable'] + 1):
                    #uncomment next line to make a combination with no stack replacement
                    #combinations.append([i, n_electrolysers, j, n_tanks])
                    for s in stack_year_options['ReplacementYearOptions']:
                        combinations.append([i, n_electrolysers, j, n_tanks])
                        s = str(s).split(',')
                        if int(s[0]) > -1:
                            for year in s:
                                combinations[-1].append(int(year))

    return combinations


