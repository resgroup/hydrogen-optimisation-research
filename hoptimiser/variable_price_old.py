import numpy as np
import pandas as pd
import datetime
import sys

from control_algorithm import LPcontrol, BasicControlDay, NoStorageDay
from component_inputs_reader import read_component_data, populate_combinations
from component_classes import CombinedElectrolyser, CombinedTank
from read_time_series_data import read_ts_data

def variable_price_runner(tank_df, electrolyser_df, data_years, input_combination):

    print('Running using combination: ', input_combination)

    start_time = datetime.datetime.now()
    lp_solver_time_limit_seconds = 0.5

    #Todo: the code assumes that we pay per MWh at the POI, and then counts line losses from there to the electrolyser.
    # Need to confirm that this is right

    grid_import_max_power = 23000
    power_factor = 0.94
    line_losses_after_poi = 0.95
    h2_heating_value = 'lower'
    discount_rate_percent = 12.0
    water_price_per_litre = 0.002
    water_needed_per_mwh_h2 = 601

    daily_capacity_charge = 46.1
    daily_fixed_charge = 569.0
    daily_TNUOS_charge = 601.76
    sleeving_fee = 0.05

    daily_fixed_charge_total = (daily_capacity_charge * grid_import_max_power / (1000 * power_factor)) + daily_fixed_charge + daily_TNUOS_charge
    annual_fixed_charge = daily_fixed_charge_total * 365.25
    annual_additonal_costs = annual_fixed_charge #todo annual_additonal_costs total so far only includes FixedNonComm from results sheet. We need to include ImportNonComm and OffsiteNonComm costs
    annual_admin_costs = annual_additonal_costs * sleeving_fee

    input_demand_profiles = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\inputs\demand_profiles.csv'
    input_price_profiles = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\inputs\price_profiles.csv'

    if h2_heating_value == 'lower':
        kwh_per_kg = 33.3
    else:
        kwh_per_kg = 39.3

    data = read_ts_data(input_demand_profiles, input_price_profiles)

    #data = data[0:48*10]

    first_operational_year = data_years.loc[0,'CalendarYear']
    n_years = len(data_years)

    selected_electrolyser = electrolyser_df.loc[input_combination[0], :]
    n_electrolysers = input_combination[1]
    selected_tank = tank_df.loc[input_combination[2], :]
    n_tanks = input_combination[3]
    stack_replacement_years = input_combination[4:]

    electrolyser = CombinedElectrolyser(selected_electrolyser, n_electrolysers, stack_replacement_years, first_operational_year, n_years, electrolyser_min_capacity = 0.1)
    tank = CombinedTank(selected_tank, n_tanks)

    electrolyser.combined_stack_and_efficiencies_df = electrolyser.combined_stack_and_efficiencies_df.merge(data_years)

    unique_years = electrolyser.combined_stack_and_efficiencies_df.groupby('combined').min().reset_index()[['combined','PriceYear','DemandYear','final_relative_efficiency']]
    unique_years = unique_years.rename(columns = {'final_relative_efficiency': 'minimum_relative_efficiency'})

    results_years = electrolyser.combined_stack_and_efficiencies_df.copy()
    results_years = results_years.drop(['PriceYear', 'DemandYear'], axis = 1)
    results_years = results_years.merge(unique_years, on = 'combined')
    results_years['cost_reduction_factor'] = results_years['minimum_relative_efficiency'] / results_years['final_relative_efficiency']

    failed_combination_flag = False

    for analysis_year in range(0, len(unique_years)):

        price_year = unique_years.loc[analysis_year, 'PriceYear']
        demand_year = unique_years.loc[analysis_year, 'DemandYear']
        efficiency_adjustment = unique_years.loc[analysis_year, 'minimum_relative_efficiency']
        data['demand'] = data[demand_year]
        data['price'] = data[str(price_year)]

        electrolyser.max_power = min(grid_import_max_power * line_losses_after_poi, electrolyser.max_power)

        p80_demand = np.percentile(data.demand, 80)
        max_h2_production = electrolyser.max_power * electrolyser.efficiency[-1] * 0.5 * efficiency_adjustment

        if (p80_demand < max_h2_production) and not (failed_combination_flag):

            print('Demand Year: ', demand_year)
            print('Price Year: ', price_year)
            print('Efficiency Adjustment based on worst year: ', efficiency_adjustment)

            #calculate the max culmulative undersupply of h2 over any rolling time period:
            data['under_supply'] = data['demand'] - max_h2_production
            max_cumulative_undersupply = 0
            for i in range(1, 48 * 10):
                #i = 48
                test = max(data['under_supply'].rolling(i).sum().shift(-(i - 1)))
                max_cumulative_undersupply = max(test, max_cumulative_undersupply)
                #if test >  0:
                #    print(i, test)
            if tank.min_storage_kwh < max_cumulative_undersupply:
                print('Minimum storage remaining set to cover worst day of over-demand: ', round(max_cumulative_undersupply, 1), ' kWh')
                tank.min_storage_kwh = max_cumulative_undersupply
                tank.starting_storage_kwh = max(tank.starting_storage_kwh, tank.min_storage_kwh)

                if tank.min_storage_kwh >= tank.max_storage_kwh:
                    failed_combination_flag = True
                    print('Tank not large enough to cover largest period of overdemand!')

            print('Electrolysers Max Power = ', round(electrolyser.max_power, 0))
            print('Max h2 production = ', round(max_h2_production, 0))
            print('Max demand = ', round(max(data['demand']), 0))
            print('P99 demand = ', round(np.percentile(data.demand, 99), 0))
            print('P95 demand = ', round(np.percentile(data.demand, 95), 0))
            print('Maximum storage kWh = ', round(tank.max_storage_kwh, 0))

            #todo lookup floor area of electrolysers and tanks and ensure it doesn't exceed the max floor area

            total_cost = 0
            day_start_h2_in_storage_kwh = tank.starting_storage_kwh

            results_df = pd.DataFrame(columns=['datetime','price','h2_demand_kWh','h2_produced_kWh','h2_to_storage_kWh','h2_in_storage_kWh'])

            i = 0
            days_with_solver_time_curtailed = 0
            day_start_storage_remaining = pd.DataFrame(columns=['remaining'])

            end_of_day_storage_target = tank.min_storage_kwh
            end_of_day_storage_increase_per_day = 0

            for day in data['Day'].unique()[0:len(data['Day'].unique())-1]:

                if not failed_combination_flag:

                    day_start_time = datetime.datetime.now()

                    day_results_df = pd.DataFrame(columns=['datetime','price','h2_demand_kWh','h2_produced_kWh','h2_to_storage_kWh','h2_in_storage_kWh'])

                    data_day = data.loc[(data['Day'] == day), :]

                    #Guess that first 12 hours of following day will have the same price and demand as this day:

                    data_day = data_day.reset_index()
                    data_day = pd.concat([data_day, data_day.loc[0:23, :]])
                    data_day = data_day.reset_index()

                    date_array = data_day.Time
                    price_array = data_day.price
                    demand_array = data_day.demand

                    day_results_df, solver_time, end_of_day_storage_target, end_of_day_storage_increase_per_day, failed_combination_flag = LPcontrol(data_day, date_array, price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, line_losses_after_poi, lp_solver_time_limit_seconds, electrolyser, tank, efficiency_adjustment, end_of_day_storage_target, end_of_day_storage_increase_per_day, max_h2_production, failed_combination_flag)

                if not failed_combination_flag:

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

                    if end_of_day_storage_target < tank.min_storage_kwh:
                        end_of_day_storage_target += end_of_day_storage_increase_per_day


            if not failed_combination_flag:

                print('Days curtailed due to solver time limit = ',days_with_solver_time_curtailed)

                results_df.to_csv('../results/results_'+str(analysis_year)+'_'+str(input_combination)+'.csv')

                day_start_storage_remaining.to_csv('daystart.csv')
                end_time = datetime.datetime.now()

                time_taken = end_time - start_time

                print('Total time taken = ', time_taken)

                combined_lookup = unique_years['combined'][analysis_year]

                results_years.loc[results_years['combined'] == combined_lookup, 'electricity_cost'] = round(total_cost, 2) * results_years.loc[results_years['combined'] == combined_lookup, 'cost_reduction_factor']
                results_years.loc[results_years['combined'] == combined_lookup, 'h2_to_demand_kWh'] = round(data.demand.sum(),2)
                results_years.loc[results_years['combined'] == combined_lookup, 'h2_to_demand_kg'] = round(data.demand.sum(),2) / kwh_per_kg
                results_years.loc[results_years['combined'] == combined_lookup, 'water_cost'] = round(data.demand.sum(),2) * water_price_per_litre * water_needed_per_mwh_h2 / 1000

            else:
                print('Combination failed to Solve')

        else:
            print('Max production insufficient to meet the P80 demand level!')
            failed_combination_flag = True

    if not failed_combination_flag:

        results_years['discount_factor'] = 1 / ((1 + discount_rate_percent / 100) ** (results_years['OperationalYear'] - 1))
        results_years['electrolyser_capex'] = 0
        results_years['tank_capex'] = 0
        results_years['stack_capex'] = electrolyser.combined_stack_and_efficiencies_df['stack_replacement'] * electrolyser.combined_stack_and_efficiencies_df['stack_final_capex']
        results_years['electrolyser_opex_yr'] = electrolyser.opex_per_year
        results_years['tank_opex_yr'] = tank.opex_per_year
        results_years.loc[0,'electrolyser_capex'] = electrolyser.capex
        results_years.loc[0, 'tank_capex'] = tank.capex
        results_years['other_energy_costs'] = annual_additonal_costs + annual_admin_costs
        #todo calculate other energy costs e.g. using sleeving fee etc.

        levilised_cost = np.dot(results_years['electricity_cost'], results_years['discount_factor'])
        levilised_cost += np.dot(results_years['other_energy_costs'], results_years['discount_factor'])
        levilised_cost += np.dot(results_years['electrolyser_capex'], results_years['discount_factor'])
        levilised_cost += np.dot(results_years['electrolyser_opex_yr'], results_years['discount_factor'])
        levilised_cost += np.dot(results_years['stack_capex'], results_years['discount_factor'])
        levilised_cost += np.dot(results_years['tank_capex'], results_years['discount_factor'])
        levilised_cost += np.dot(results_years['tank_opex_yr'], results_years['discount_factor'])
        levilised_cost += np.dot(results_years['water_cost'], results_years['discount_factor'])

        levilised_production = np.dot(results_years['h2_to_demand_kg'], results_years['discount_factor'])

        lcoh2 = levilised_cost / levilised_production

        print('lcoh2 = ', lcoh2)

        results_years.to_csv('../results/overall_results'+str(input_combination)+'.csv')

    else:
        lcoh2 = -99.99

    return lcoh2