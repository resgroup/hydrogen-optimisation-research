import numpy as np
import pandas as pd
import datetime
import sys

from control_algorithm import LPcontrol, BasicControlDay, NoStorageDay
from component_inputs_reader import read_component_data, populate_combinations
from component_classes import CombinedElectrolyser, CombinedTank

if __name__ == "__main__":

    start_time = datetime.datetime.now()
    lp_solver_time_limit_seconds = 1.0

    input_file_name_demand_price = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\examples\Example_demand.csv'
    input_file_name_components = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\examples\component_inputs.xlsx'

    data = pd.read_csv(input_file_name_demand_price)
    data.Time = pd.to_datetime(data.Time, dayfirst=True)
    data['Hour'] = data.Time.dt.hour
    data['Day'] = data.Time.dt.date

    tank_df, electrolyser_df = read_component_data(input_file_name_components)
    combinations = populate_combinations(tank_df, electrolyser_df)
    test_combination = combinations[0]
    test_combination = [1, 3, 0, 4]

    grid_import_max_power = 40000
    line_losses_after_poi = 0.95

    selected_electrolyser = electrolyser_df.loc[test_combination[0], :]
    n_electrolysers = test_combination[1]
    selected_tank = tank_df.loc[test_combination[2], :]
    n_tanks = test_combination[3]

    electrolyser = CombinedElectrolyser(selected_electrolyser, n_electrolysers)
    tank = CombinedTank(selected_tank, n_tanks)

    p75_demand = np.percentile(data.Demand, 75)
    max_h2_production = electrolyser.max_power * electrolyser.efficiency[-1] * 0.5

    if max_h2_production < p75_demand:
        raise Exception('Production capacity unlikely to be able to meet demand!')

    print('Electrolysers Max Power = ', electrolyser.max_power)
    print('Maximum storage kWh = ', tank.max_storage_kwh)

    #todo lookup floor area of electrolysers and tanks and ensure it doesn't exceed the max floor area

    electrolyser.max_power = min(grid_import_max_power * line_losses_after_poi, electrolyser.max_power)

    total_cost = 0
    day_start_h2_in_storage_kwh = tank.starting_storage_kwh

    results_df = pd.DataFrame(columns=['datetime','price','h2_demand_kWh','h2_produced_kWh','h2_to_storage_kWh','h2_in_storage_kWh'])

    i = 0
    days_with_solver_time_curtailed = 0
    day_start_storage_remaining = pd.DataFrame(columns=['remaining'])

    for day in data['Day'].unique()[0:len(data['Day'].unique())-1]:

        day_start_time = datetime.datetime.now()

        day_results_df = pd.DataFrame(columns=['datetime','price','h2_demand_kWh','h2_produced_kWh','h2_to_storage_kWh','h2_in_storage_kWh'])

        data_day = data.loc[(data['Day'] == day), :]

        #Guess that first 12 hours of following day will have the same price and demand as this day:

        data_day = data_day.reset_index()
        data_day = pd.concat([data_day, data_day.loc[0:23, :]])
        data_day = data_day.reset_index()

        date_array = data_day.Time
        price_array = data_day.Price
        demand_array = data_day.Demand

        day_results_df, solver_time = LPcontrol(data_day, date_array, price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, line_losses_after_poi, lp_solver_time_limit_seconds, electrolyser, tank)

        #day_results_df = BasicControlDay(date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, min_h2_production_kwh, max_h2_production_kwh, max_h2_to_storage_kwh, max_storage_kwh, min_storage_kwh)

        #day_results_df = NoStorageDay(date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh)

        day_start_h2_in_storage_kwh = day_results_df['h2_in_storage_kWh'][47]

        day_start_storage_remaining.loc[i, 'date'] = day_results_df['datetime'][47]
        day_start_storage_remaining.loc[i,'remaining'] = day_start_h2_in_storage_kwh
        i += 1
        total_cost += day_results_df['h2_cost_corrected'].sum()

        results_df = pd.concat([results_df, day_results_df])

        day_end_time = datetime.datetime.now()

        day_time_taken = day_end_time - day_start_time

        if solver_time >= datetime.timedelta(seconds = lp_solver_time_limit_seconds) * 0.999:
            days_with_solver_time_curtailed += 1

    print('Days curtailed due to solver time limit = ',days_with_solver_time_curtailed)

    print(total_cost/1000000)

    results_df.to_csv('results.csv')

    day_start_storage_remaining.to_csv('daystart.csv')
    end_time = datetime.datetime.now()

    time_taken = end_time - start_time

    print('Total time taken = ', time_taken)
