import pandas as pd
import numpy as np

def read_component_data(input_file_name):

    tank_df = pd.read_excel(input_file_name, sheet_name='Tanks')
    electrolyser_df = pd.read_excel(input_file_name, sheet_name='Electrolysers')
    efficiency_load_factor_df = pd.read_excel(input_file_name, sheet_name='EfficiencyLoadFactorCurves')

    tank_df = tank_df[tank_df['Maximum Selectable'] > 0]
    electrolyser_df = electrolyser_df[electrolyser_df['Maximum Selectable'] > 0]

    tank_df = tank_df.reset_index()
    electrolyser_df = electrolyser_df.reset_index()
    electrolyser_df['electrolyser_efficiency'] = np.empty((len(electrolyser_df), 0)).tolist()
    electrolyser_df['electrolyser_efficiency_load_factors'] = np.empty((len(electrolyser_df), 0)).tolist()

    for i in range(0, len(electrolyser_df)):
        electrolyser_df.loc[i, 'electrolyser_efficiency'].append(efficiency_load_factor_df[electrolyser_df.loc[i, 'Efficiency Load Factor Curve']].to_list())
        electrolyser_df.loc[i, 'electrolyser_efficiency_load_factors'].append(efficiency_load_factor_df['Load Factor'].to_list())

    return tank_df, electrolyser_df

def populate_combinations(tank_df, electrolyser_df):

    combinations = []

    for i in range(0, len(electrolyser_df)):
        for n_electrolysers in range(max(1, electrolyser_df.loc[i, 'Minimum Selectable']), electrolyser_df.loc[i, 'Maximum Selectable']+1):
            for j in range(0, len(tank_df)):
                for n_tanks in range(max(1, tank_df.loc[j, 'Minimum Selectable']), tank_df.loc[j, 'Maximum Selectable'] + 1):
                    combinations.append([i, n_electrolysers, j, n_tanks])

    return combinations


