import numpy as np
import pandas as pd
import datetime
import os
import sys
import json
from scipy.stats import percentileofscore

try:
    from hoptimiser.control_algorithm import LPcontrol5, LPcontrol10
    from hoptimiser.component_inputs_reader import read_component_data, populate_combinations
    from hoptimiser.component_classes import CombinedElectrolyser, CombinedTank
    from hoptimiser.read_time_series_data import read_ts_data
    from hoptimiser.config import PROJECT_ROOT_DIR
except:
    from control_algorithm import LPcontrol5, LPcontrol10
    from component_inputs_reader import read_component_data, populate_combinations
    from component_classes import CombinedElectrolyser, CombinedTank
    from read_time_series_data import read_ts_data
    from config import PROJECT_ROOT_DIR


class Analysis():

    def __init__(self, input_combination: list, run_in_azure: bool):

        input_combination_list = [int(el) for el in input_combination[1:-1].split(',')]

        self.input_combination = input_combination_list
        self.run_in_azure = run_in_azure

    def run(self):

        if self.run_in_azure:
            input_dir = PROJECT_ROOT_DIR
            output_dir_high_level = PROJECT_ROOT_DIR
        else:
            input_dir = os.path.join(PROJECT_ROOT_DIR, 'inputs')
            output_dir_high_level = os.path.join(PROJECT_ROOT_DIR, 'results')

        input_file_name_components = os.path.join(
            input_dir,
            'component_inputs.xlsx',
        )
        input_demand_profiles = os.path.join(
            input_dir,
            'demand_profiles.csv',
        )
        input_price_profiles = os.path.join(
            input_dir,
            'price_profiles.csv',
        )

        tank_df, electrolyser_df, data_years = read_component_data(input_file_name_components)

        start_time = datetime.datetime.now()

        #Todo: the code assumes that we pay per MWh at the POI, and then counts line losses from there to the electrolyser.
        # Need to confirm that this is right

        economic_inputs = pd.read_excel(input_file_name_components, sheet_name='Economic Inputs')
        economic_inputs.set_index('Parameter', inplace=True)

        technical_inputs = pd.read_excel(input_file_name_components, sheet_name='Technical Inputs')
        technical_inputs.set_index('Parameter', inplace=True)

        capital_cost_baseline_year = economic_inputs['Value']['Capital Cost Baseline Year']
        capital_cost_price_year = economic_inputs['Value']['Capital Cost Price Year']
        electricity_price_year = economic_inputs['Value']['Electricity Price Baseline Year']
        if capital_cost_price_year == electricity_price_year:
            combined_elec_price_inflation = 1.0
        else:
            combined_elec_price_inflation = economic_inputs['Value']['Combined Electricity Price Inflation'] # to inflate elec price input data to match price year of capital costs

        #todo consider adding forward price annual averages and scaling price time series to match these

        grid_import_max_power = technical_inputs['Value']['Max Grid Import Power MW']
        power_factor = technical_inputs['Value']['Power Factor']
        line_efficiency_after_poi = technical_inputs['Value']['Line Efficiency after POI'] #todo check this
        start_half_full = bool(technical_inputs['Value']['Start With Tanks Half Full'])
        tank_min_storage_limit = technical_inputs['Value']['Tank Min Storage Level Allowed']
        h2_heating_value = economic_inputs['Value']['Heating Value'].lower()
        discount_rate_percent = economic_inputs['Value']['Discount Rate %']
        water_price_per_litre = economic_inputs['Value']['Water Price Per Litre']
        water_needed_per_mwh_h2 = economic_inputs['Value']['Water Litres Per MWh']
        electrolyser_min_capacity = technical_inputs['Value']['Electrolyser Min Operating Capacity']
        daily_capacity_charge = economic_inputs['Value']['Daily Capacity Charge per MVA (£)']
        daily_fixed_charge = economic_inputs['Value']['Daily Fixed Charge (£)']
        daily_TNUOS_charge = economic_inputs['Value']['Daily TNUOS Charge (£)']
        sleeving_fee = economic_inputs['Value']['Sleeving Fee Percentage']
        supplier_fee = economic_inputs['Value']['Supplier Fee per MWh Imported (£)']
        reduce_efficiencies = bool(technical_inputs['Value']['Simplify Efficiencies to Five Points']) #If true, efficiency table will be simplified to 5 rows from 10
        lp_solver_time_limit_seconds = technical_inputs['Value']['Linear Solver Per Day Time Limit (s)']
        allow_for_offline_electrolyser = False

        daily_fixed_charge_total = (daily_capacity_charge * grid_import_max_power / power_factor) + daily_fixed_charge + daily_TNUOS_charge
        annual_fixed_charge = daily_fixed_charge_total * 365.25
        #todo annual_additonal_costs total so far only includes FixedNonComm from results sheet. We may need to include ImportNonComm (time of use charges) and OffsiteNonComm costs
        annual_admin_costs = annual_fixed_charge * sleeving_fee/100

        if h2_heating_value == 'lower':
            kwh_per_kg = 33.3
        else:
            kwh_per_kg = 39.3

        data = read_ts_data(input_demand_profiles, input_price_profiles, input_file_name_components)

        first_operational_year = data_years.loc[0,'CalendarYear']
        n_years = len(data_years)

        print(self.input_combination)
        selected_electrolyser = electrolyser_df.loc[self.input_combination[0], :]
        n_electrolysers = self.input_combination[1]
        print('\nRunning with '+str(n_electrolysers)+' electrolysers of the following model:')
        print(selected_electrolyser)

        selected_tank = tank_df.loc[self.input_combination[2], :]
        n_tanks = self.input_combination[3]
        print('\nRunning with ' + str(n_tanks) + ' tanks of the following model:')
        print(selected_tank)
        stack_replacement_years = self.input_combination[4:]

        electrolyser = CombinedElectrolyser(selected_electrolyser, n_electrolysers, stack_replacement_years, first_operational_year, capital_cost_baseline_year, n_years, electrolyser_min_capacity=electrolyser_min_capacity, reduce_efficiencies=reduce_efficiencies, optimise_efficiencies=False)
        tank = CombinedTank(selected_tank, n_tanks, min_storage_kwh = tank_min_storage_limit, start_half_full=start_half_full)

        electrolyser.combined_stack_and_efficiencies_df = electrolyser.combined_stack_and_efficiencies_df.merge(data_years, how='left')

        unique_years = electrolyser.combined_stack_and_efficiencies_df.groupby('combined').min().reset_index()[['combined','PriceYear','DemandYear','final_relative_efficiency']]
        unique_years = unique_years.rename(columns = {'final_relative_efficiency': 'minimum_relative_efficiency'})

        results_years = electrolyser.combined_stack_and_efficiencies_df.copy()
        results_years = results_years.drop(['PriceYear', 'DemandYear'], axis = 1)
        results_years = results_years.merge(unique_years, on = 'combined', how = 'left')
        results_years['cost_reduction_factor'] = results_years['minimum_relative_efficiency'] / results_years['final_relative_efficiency']

        failed_combination_flag = False
        output_dir = str(self.input_combination)[1:-1].replace(",", "_").replace(" ", "")

        dir_to_create = os.path.join(output_dir_high_level,output_dir)

        if not self.run_in_azure:
            if not os.path.exists(dir_to_create):
                os.mkdir(dir_to_create)

        for analysis_year in range(0, len(unique_years)):

            price_year = int(unique_years.loc[analysis_year, 'PriceYear'])
            demand_year = unique_years.loc[analysis_year, 'DemandYear']
            efficiency_adjustment = unique_years.loc[analysis_year, 'minimum_relative_efficiency']
            data['demand'] = data[demand_year]
            data['import_price'] = data[str(price_year)] * combined_elec_price_inflation# * line_efficiency_after_poi #todo line losses required to match HYDRA appraoch
            data['combined_price'] = data['import_price'] + data['uos_charge']

            electrolyser.max_power = min(grid_import_max_power * 1000 * line_efficiency_after_poi, electrolyser.rated_power)

            p80_demand = np.percentile(data.demand, 80)
            max_h2_production = electrolyser.max_power * electrolyser.efficiency[-1] * 0.5 * efficiency_adjustment
            max_h2_production_one_offline = max_h2_production * (n_electrolysers - 1)/n_electrolysers

            if (p80_demand < max_h2_production) and not (failed_combination_flag):

                print('\nDemand Year: ', demand_year)
                print('Price Year: ', price_year)
                print('Efficiency Adjustment based on worst year: ', efficiency_adjustment)

                #calculate the max culmulative undersupply of h2 over any rolling time period:
                if allow_for_offline_electrolyser:
                    data['under_supply'] = data['demand'] - max_h2_production_one_offline
                else:
                    data['under_supply'] = data['demand'] - max_h2_production

                max_cumulative_undersupply = 0
                for i in range(1, 48 * 10):
                    test = max(data['under_supply'].rolling(i).sum().shift(-(i - 1)))
                    max_cumulative_undersupply = max(test, max_cumulative_undersupply)

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
                total_import_cost = 0
                total_uos_cost = 0
                total_supplier_fee_costs = 0
                total_h2_produced = 0
                h2_price_sum_product = 0

                day_start_h2_in_storage_kwh = tank.starting_storage_kwh

                results_df = pd.DataFrame()

                i = 0
                days_with_solver_time_curtailed = 0
                day_start_storage_remaining = pd.DataFrame(columns=['remaining'])

                end_of_day_storage_target = tank.min_storage_kwh
                end_of_day_storage_increase_per_day = 0

                for day in data['Day'].unique()[0:len(data['Day'].unique())]:

                    if not failed_combination_flag:

                        data_day = data.loc[(data['Day'] == day), :]

                        #Guess that first 12 hours of following day will have the same price and demand as this day:

                        data_day = data_day.reset_index()
                        data_day = data_day.drop(columns=['level_0', 'index'])
                        data_day = pd.concat([data_day, data_day.loc[0:23, :]])
                        data_day = data_day.reset_index()


                        if reduce_efficiencies:
                            day_results_df, solver_time, end_of_day_storage_target, end_of_day_storage_increase_per_day, failed_combination_flag, mean_production_price = LPcontrol5(data_day, day_start_h2_in_storage_kwh, line_efficiency_after_poi, lp_solver_time_limit_seconds, electrolyser, tank, efficiency_adjustment, end_of_day_storage_target, end_of_day_storage_increase_per_day, max_h2_production, failed_combination_flag, supplier_fee)
                        else:

                            day_results_df, solver_time, end_of_day_storage_target, end_of_day_storage_increase_per_day, failed_combination_flag, mean_production_price = LPcontrol10(data_day, day_start_h2_in_storage_kwh, line_efficiency_after_poi, lp_solver_time_limit_seconds, electrolyser, tank, efficiency_adjustment, end_of_day_storage_target, end_of_day_storage_increase_per_day, max_h2_production, failed_combination_flag, supplier_fee)


                    if not failed_combination_flag:

                        day_start_h2_in_storage_kwh = day_results_df['h2_in_storage_kWh'][47]
                        day_start_storage_remaining.loc[i, 'date'] = day_results_df['datetime'][47]
                        day_start_storage_remaining.loc[i,'remaining'] = day_start_h2_in_storage_kwh
                        i += 1
                        total_cost += day_results_df['h2_cost_total'].sum()
                        total_import_cost += day_results_df['h2_cost_imports'].sum()
                        total_uos_cost += day_results_df['h2_cost_uos'].sum()
                        total_supplier_fee_costs += day_results_df['h2_cost_supplier_fee'].sum()

                        day_h2_produced = day_results_df['h2_produced_kWh'].sum()

                        if not np.isnan(mean_production_price):
                            h2_price_sum_product += day_h2_produced * mean_production_price

                        total_h2_produced += day_h2_produced

                        results_df = pd.concat([results_df, day_results_df])

                        if solver_time >= datetime.timedelta(seconds = lp_solver_time_limit_seconds) * 0.999:
                            days_with_solver_time_curtailed += 1

                        if end_of_day_storage_target < tank.min_storage_kwh:
                            end_of_day_storage_target += end_of_day_storage_increase_per_day

                if not failed_combination_flag:

                    print('Days curtailed due to solver time limit = ',days_with_solver_time_curtailed)

                    weighted_mean_price_when_producing = h2_price_sum_product / total_h2_produced

                    production_price_percentile = percentileofscore(data['import_price'], weighted_mean_price_when_producing)

                    results_df.to_csv(os.path.join(
                        output_dir_high_level,
                        output_dir,
                        str(analysis_year)+'_'+str(self.input_combination)+'_output_time_series.csv',
                    ))

                    combined_lookup = unique_years['combined'][analysis_year]

                    for j in range(0,len(results_years)):
                        if results_years.loc[j,'combined'] == combined_lookup:
                            scaling_prices = data[str(int(results_years.loc[j, 'PriceScaleYear']))]
                            scaling_year_value = np.percentile(scaling_prices, production_price_percentile)
                            control_year_value = np.percentile(data['import_price'], production_price_percentile)
                            price_scaling_ratio = scaling_year_value / control_year_value

                            results_years.loc[j, 'import_elec_cost'] = round(total_import_cost * results_years.loc[j, 'cost_reduction_factor'] * price_scaling_ratio, 2)
                            results_years.loc[j, 'uos_elec_cost'] = round(total_uos_cost * results_years.loc[j, 'cost_reduction_factor'], 2)
                            results_years.loc[j, 'total_elec_cost'] = round(results_years.loc[j, 'import_elec_cost'] + results_years.loc[j, 'uos_elec_cost'], 2)
                            results_years.loc[j, 'supplier_fee_cost'] = round(total_supplier_fee_costs, 2) * results_years.loc[j, 'cost_reduction_factor']
                            results_years.loc[j, 'h2_to_demand_kWh'] = round(data.demand.sum(),2)
                            results_years.loc[j, 'h2_to_demand_kg'] = round(data.demand.sum(),2) / kwh_per_kg
                            results_years.loc[j, 'water_cost'] = round(data.demand.sum(),2) * water_price_per_litre * water_needed_per_mwh_h2 / 1000

                else:
                    print('Combination failed to Solve')

            else:
                print('Max production insufficient to meet the P80 demand level!')
                failed_combination_flag = True

        end_time = datetime.datetime.now()
        time_taken = end_time - start_time
        print('Total time taken = ', time_taken)


        if not failed_combination_flag:

            operational_filter = results_years['OperationalYear'] > 0

            results_years['years_since_costs_baseline'] = results_years['CalendarYear'] - min(results_years['CalendarYear'])

            results_years['discount_factor'] = 1 / ((1 + discount_rate_percent / 100) ** (results_years['years_since_costs_baseline']))
            results_years['electrolyser_capex'] = 0.0
            results_years['tank_capex'] = 0.0
            results_years['stack_capex'] = electrolyser.combined_stack_and_efficiencies_df['stack_replacement'] * electrolyser.combined_stack_and_efficiencies_df['stack_final_capex']
            results_years.loc[operational_filter, 'electrolyser_opex_yr'] = electrolyser.opex_per_year
            results_years.loc[operational_filter, 'tank_opex_yr'] = tank.opex_per_year
            results_years.loc[0,'electrolyser_capex'] = electrolyser.capex
            results_years.loc[0, 'tank_capex'] = tank.capex
            results_years.loc[operational_filter, 'other_energy_costs'] = annual_fixed_charge
            results_years.loc[operational_filter, 'sleeving_cost'] = annual_admin_costs

            results_years = results_years.fillna(0)

            levilised_cost = np.dot(results_years['total_elec_cost'], results_years['discount_factor'])
            levilised_cost += np.dot(results_years['other_energy_costs'], results_years['discount_factor'])
            levilised_cost += np.dot(results_years['sleeving_cost'], results_years['discount_factor'])
            levilised_cost += np.dot(results_years['electrolyser_opex_yr'], results_years['discount_factor'])
            levilised_cost += np.dot(results_years['stack_capex'], results_years['discount_factor'])
            levilised_cost += np.dot(results_years['tank_capex'], results_years['discount_factor'])
            levilised_cost += np.dot(results_years['tank_opex_yr'], results_years['discount_factor'])
            levilised_cost += np.dot(results_years['water_cost'], results_years['discount_factor'])

            levilised_cost += np.dot(results_years['electrolyser_capex'], results_years['discount_factor'])
            levilised_cost += np.dot(results_years['tank_capex'], results_years['discount_factor'])

            levilised_production = np.dot(results_years['h2_to_demand_kg'], results_years['discount_factor'])

            lcoh2 = levilised_cost / levilised_production

            print('lcoh2 = ', lcoh2)

            results_years.to_csv(os.path.join(
                output_dir_high_level,
                output_dir,
                str(self.input_combination) + '_output_annual_results.csv',
            ))


        else:
            lcoh2 = -99.99

        # with open(os.path.join(PROJECT_ROOT_DIR, 'results', 'lcoh2_result.json'), 'w', encoding='utf-8') as f:
        #     json.dump({
        #         'combination': self.input_combination,
        #         'lcoh2': lcoh2,
        #         'total_time_taken': str(time_taken),
        #     }, f, indent=2)

        with open(os.path.join(output_dir_high_level, output_dir, 'lcoh2_result.json'), 'w', encoding='utf-8') as f:
            json.dump({
                'combination': self.input_combination,
                'lcoh2': lcoh2,
                'total_time_taken': str(time_taken),
            }, f, indent=2)

        return lcoh2

if __name__ == "__main__":

    if len(sys.argv) == 3:
        input_combination = sys.argv[1]
        run_in_azure = sys.argv[2]
    else:
        raise Exception(f'Invalid number of command line arguments:{len(sys.argv)}')

    analysis = Analysis(
        input_combination=input_combination, run_in_azure=run_in_azure
    )

    lcoh2_this_combination = round(analysis.run(),1)
