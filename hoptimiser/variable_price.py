import numpy as np
import pandas as pd
import datetime
import sys

from control_algorithm import LPcontrol, BasicControlDay, NoStorageDay

if __name__ == "__main__":

    start_time = datetime.datetime.now()

    input_file_name = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\examples\Example_demand.csv'

    #todo make into command line tool

    data = pd.read_csv(input_file_name)
    data.Time = pd.to_datetime(data.Time, dayfirst=True)
    data['Hour'] = data.Time.dt.hour
    data['Day'] = data.Time.dt.date

    max_storage_kwh = float(sys.argv[1])
    min_storage_kwh = 2000
    storage_max_charge_rate_kw_h2 = 20000

    electrolyser_max_power = float(sys.argv[2])
    electrolyser_min_power = electrolyser_max_power * 0.1
    electrolyser_efficiency = 0.615
    starting_storage_kwh = 0

    print('Optimising control with Max Storage Capacity of '+str(max_storage_kwh)+' and Electrolyser Rated Power of '+str(electrolyser_max_power))

    electrolyser_min_power = max(electrolyser_min_power, 1E-4)
    min_storage_kwh = max(min_storage_kwh, 1E-4)

    max_h2_production_kwh = electrolyser_max_power * electrolyser_efficiency * 0.5
    min_h2_production_kwh = electrolyser_min_power * electrolyser_efficiency * 0.5
    max_h2_to_storage_kwh = min(storage_max_charge_rate_kw_h2 * 0.5, max_h2_production_kwh)

    total_cost = 0
    day_start_h2_in_storage_kwh = starting_storage_kwh

    results_df = pd.DataFrame(columns=['datetime','price','h2_demand_kWh','h2_produced_kWh','h2_to_storage_kWh','h2_in_storage_kWh'])

    solver_time_taken = datetime.timedelta(0)

    i = 0
    day_start_storage_remaining = pd.DataFrame(columns=['remaining'])
    for day in data['Day'].unique()[0:len(data['Day'].unique())-1]:

        day_results_df = pd.DataFrame(columns=['datetime','price','h2_demand_kWh','h2_produced_kWh','h2_to_storage_kWh','h2_in_storage_kWh'])

        data_day = data.loc[(data['Day'] == day), :]

        #Guess that first 12 hours of following day will have the same price and demand as this day:

        data_day = data_day.reset_index()
        data_day = pd.concat([data_day, data_day.loc[0:23, :]])
        data_day = data_day.reset_index()

        date_array = data_day.Time
        price_array = data_day.Price
        demand_array = data_day.Demand
        h2_price_array = price_array / electrolyser_efficiency

        max_h2_from_storage_kwh = min((max_storage_kwh - min_storage_kwh), max(demand_array))

        day_results_df = LPcontrol(data_day, date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, min_h2_production_kwh, max_h2_production_kwh, max_h2_from_storage_kwh, max_h2_to_storage_kwh, max_storage_kwh, min_storage_kwh)

        #day_results_df = BasicControlDay(date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, min_h2_production_kwh, max_h2_production_kwh, max_h2_to_storage_kwh, max_storage_kwh, min_storage_kwh)

        #day_results_df = NoStorageDay(date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh)

        day_start_h2_in_storage_kwh = day_results_df['h2_in_storage_kWh'][47]

        day_start_storage_remaining.loc[i, 'date'] = day_results_df['datetime'][47]
        day_start_storage_remaining.loc[i,'remaining'] = day_start_h2_in_storage_kwh
        i += 1
        total_cost += day_results_df['h2_cost'].sum()

        results_df = pd.concat([results_df, day_results_df])


    print(total_cost/1000000)

    results_df.to_csv('results.csv')

    day_start_storage_remaining.to_csv('daystart.csv')
    end_time = datetime.datetime.now()

    time_taken = end_time - start_time

    print('Solver time taken = ', solver_time_taken)
    print('Total time taken = ', time_taken)
