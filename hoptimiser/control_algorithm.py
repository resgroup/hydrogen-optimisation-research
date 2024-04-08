import numpy as np
from pulp import pulp, LpProblem, LpMinimize, PULP_CBC_CMD, COIN
import datetime

class MultiDimensionalLpVariable:
    def __init__(self, name, dimensions, low_bound, up_bound, cat):
        self.name = name
        try:
            self.dimensions = (*dimensions,)
        except Exception:
            self.dimensions = (dimensions,)
        self.low_bound = low_bound
        self.up_bound = up_bound
        self.cat = cat
        self.variables = self._build_variables_array()
        self.values = None

    def __getitem__(self, index):
        return self.variables[index]

    def _build_variables_array(self):
        f = np.vectorize(self._define_variable)
        return np.fromfunction(f, self.dimensions, dtype="int")

    def _define_variable(self, *index):
        name = "_".join(map(str, (self.name, *index)))
        return pulp.LpVariable(name, self.low_bound, self.up_bound, self.cat)

    def evaluate(self):
        f = np.vectorize(lambda i: pulp.value(i))
        self.values = f(self.variables)

def LPcontrol(data_day, date_array, price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, line_losses_after_poi, lp_solver_time_limit_seconds, electrolyser, tank, efficiency_adjustment):

    cost = 0

    electrolyser_kW_level_1 = MultiDimensionalLpVariable('electrolyser_kW_1', len(data_day), 0, electrolyser.efficiency_load_factor_simplified[0] * electrolyser.max_power, "Continuous")
    electrolyser_kW_level_2 = MultiDimensionalLpVariable('electrolyser_kW_2', len(data_day), 0, electrolyser.efficiency_load_factor_simplified[1] * electrolyser.max_power, "Continuous")
    electrolyser_kW_level_3 = MultiDimensionalLpVariable('electrolyser_kW_3', len(data_day), 0, electrolyser.efficiency_load_factor_simplified[2] * electrolyser.max_power, "Continuous")
    electrolyser_kW_level_4 = MultiDimensionalLpVariable('electrolyser_kW_4', len(data_day), 0, electrolyser.efficiency_load_factor_simplified[3] * electrolyser.max_power, "Continuous")
    electrolyser_kW_level_5 = MultiDimensionalLpVariable('electrolyser_kW_5', len(data_day), 0, electrolyser.efficiency_load_factor_simplified[4] * electrolyser.max_power, "Continuous")

    electrolyser_turned_on_level_1 = MultiDimensionalLpVariable('electrolyser_turned_on_1', len(data_day), 0, 1, cat = "Binary")
    electrolyser_turned_on_level_2 = MultiDimensionalLpVariable('electrolyser_turned_on_2', len(data_day), 0, 1, cat = "Binary")
    electrolyser_turned_on_level_3 = MultiDimensionalLpVariable('electrolyser_turned_on_3', len(data_day), 0, 1, cat = "Binary")
    electrolyser_turned_on_level_4 = MultiDimensionalLpVariable('electrolyser_turned_on_4', len(data_day), 0, 1, cat = "Binary")
    electrolyser_turned_on_level_5 = MultiDimensionalLpVariable('electrolyser_turned_on_5', len(data_day), 0, 1, cat = "Binary")

    adjusted_efficiency_1 = electrolyser.efficiency_simplified[0] * efficiency_adjustment
    adjusted_efficiency_2 = electrolyser.efficiency_simplified[1] * efficiency_adjustment
    adjusted_efficiency_3 = electrolyser.efficiency_simplified[2] * efficiency_adjustment
    adjusted_efficiency_4 = electrolyser.efficiency_simplified[3] * efficiency_adjustment
    adjusted_efficiency_5 = electrolyser.efficiency_simplified[4] * efficiency_adjustment

    adjusted_full_efficiency_curve = [e * efficiency_adjustment for e in electrolyser.efficiency]

    h2_in_storage_kwh = day_start_h2_in_storage_kwh

    problem = LpProblem("Minimize_energy_cost_while_meeting_demand", LpMinimize)

    for i in range(0, len(price_array)):
        electrolyser_kW = electrolyser_kW_level_1[i] + electrolyser_kW_level_2[i] + electrolyser_kW_level_3[i] + electrolyser_kW_level_4[i] + electrolyser_kW_level_5[i]
        cost += (electrolyser_kW / line_losses_after_poi) * price_array[i] * 0.5 / 1000.

    problem += cost

    for i in range(0, len(price_array)):

        h2_produced_kWh = 0.5 * ((electrolyser_kW_level_1[i] * adjusted_efficiency_1) + (electrolyser_kW_level_2[i] * adjusted_efficiency_2) + (electrolyser_kW_level_3[i] * adjusted_efficiency_3) + (electrolyser_kW_level_4[i] * adjusted_efficiency_4) + (electrolyser_kW_level_5[i] * adjusted_efficiency_5))

        h2_storage_space_available_kwh = tank.max_storage_kwh - h2_in_storage_kwh

        h2_to_storage = h2_produced_kWh - demand_array[i]
        storage_remaining = h2_in_storage_kwh + h2_to_storage
        storage_overfill = h2_to_storage - h2_storage_space_available_kwh

        h2_in_storage_kwh = (h2_in_storage_kwh + h2_to_storage) * tank.remaining_fraction_after_half_hour

        problem += storage_remaining >= tank.min_storage_kwh
        problem += storage_overfill <= 0

        problem += electrolyser_kW_level_1[i] <= electrolyser_turned_on_level_1[i] * electrolyser.efficiency_load_factor_simplified[0] * electrolyser.max_power
        problem += electrolyser_kW_level_1[i] >= electrolyser_turned_on_level_1[i] * electrolyser.min_power

        problem += electrolyser_kW_level_2[i] <= electrolyser_turned_on_level_2[i] * electrolyser.efficiency_load_factor_simplified[1] * electrolyser.max_power
        problem += electrolyser_kW_level_2[i] >= electrolyser_turned_on_level_2[i] * electrolyser.efficiency_load_factor_simplified[0] * electrolyser.max_power

        problem += electrolyser_kW_level_3[i] <= electrolyser_turned_on_level_3[i] * electrolyser.efficiency_load_factor_simplified[2] * electrolyser.max_power
        problem += electrolyser_kW_level_3[i] >= electrolyser_turned_on_level_3[i] * electrolyser.efficiency_load_factor_simplified[1] * electrolyser.max_power

        problem += electrolyser_kW_level_4[i] <= electrolyser_turned_on_level_4[i] * electrolyser.efficiency_load_factor_simplified[3] * electrolyser.max_power
        problem += electrolyser_kW_level_4[i] >= electrolyser_turned_on_level_4[i] * electrolyser.efficiency_load_factor_simplified[2] * electrolyser.max_power

        problem += electrolyser_kW_level_5[i] <= electrolyser_turned_on_level_5[i] * electrolyser.efficiency_load_factor_simplified[4] * electrolyser.max_power
        problem += electrolyser_kW_level_5[i] >= electrolyser_turned_on_level_5[i] * electrolyser.efficiency_load_factor_simplified[3] * electrolyser.max_power

        problem += electrolyser_turned_on_level_1[i] + electrolyser_turned_on_level_2[i] + electrolyser_turned_on_level_3[i] + electrolyser_turned_on_level_4[i] + electrolyser_turned_on_level_5[i] <= 1

    start_solver_time = datetime.datetime.now()

    problem.solve(PULP_CBC_CMD(msg=False, keepFiles=False, timeLimit=lp_solver_time_limit_seconds))

    end_solver_time = datetime.datetime.now()

    solver_time = end_solver_time - start_solver_time

    if not problem.status == 1:
    #    raise Exception('Problem is infeasible for at least one time period')
        print('Infeasible day:', data_day['Day'][0])


    # b = COIN(timeLimit=100.0, msg=False)
    # b.path = "cbc.exe"
    # b.solve(problem)

    electrolyser_kW_level_1.evaluate()
    electrolyser_kW_level_2.evaluate()
    electrolyser_kW_level_3.evaluate()
    electrolyser_kW_level_4.evaluate()
    electrolyser_kW_level_5.evaluate()

    electrolyser_kW_result = electrolyser_kW_level_1.values + electrolyser_kW_level_2.values + electrolyser_kW_level_3.values + electrolyser_kW_level_4.values + electrolyser_kW_level_5.values

    h2_in_storage_tracker = day_start_h2_in_storage_kwh
    h2_in_storage = np.zeros(48)
    h2_to_storage = np.zeros(48)
    cost_array = np.zeros(48)
    corrected_cost_array = np.zeros(48)

    h2_produced_kWh_result = price_array.copy()

    for i in range(0,48):

        h2_produced_kWh_result[i] = 0.5 * ((electrolyser_kW_level_1.values[i] * adjusted_efficiency_1) + (electrolyser_kW_level_2.values[i] * adjusted_efficiency_2) + (electrolyser_kW_level_3.values[i] * adjusted_efficiency_3) + (electrolyser_kW_level_4.values[i] * adjusted_efficiency_4) + (electrolyser_kW_level_5.values[i] * adjusted_efficiency_5))
        h2_to_storage[i] = h2_produced_kWh_result[i] - demand_array[i]
        real_efficiency = np.interp(electrolyser_kW_result[i] / electrolyser.max_power, electrolyser.efficiency_load_factor, adjusted_full_efficiency_curve)
        cost_array[i] = (electrolyser_kW_result[i] / (1000 * line_losses_after_poi)) * price_array[i] * 0.5
        corrected_cost_array[i] = (h2_produced_kWh_result[i] * price_array[i]) / (1000 * real_efficiency * line_losses_after_poi)
        h2_in_storage_tracker = (h2_in_storage_tracker + h2_to_storage[i]) * tank.remaining_fraction_after_half_hour
        h2_in_storage[i] = h2_in_storage_tracker

    if any(h < 0 for h in h2_in_storage):
        raise Exception('Negative storage found - control unsolveable!')

    day_results_df['datetime'] = date_array[0:48]
    day_results_df['price'] = price_array[0:48]
    day_results_df['h2_demand_kWh'] = demand_array[0:48]
    day_results_df['electrolyser_kW'] = electrolyser_kW_result[0:48]
    day_results_df['electrolyser_kW_1'] = electrolyser_kW_level_1.values[0:48]
    day_results_df['electrolyser_kW_2'] = electrolyser_kW_level_2.values[0:48]
    day_results_df['electrolyser_kW_3'] = electrolyser_kW_level_3.values[0:48]
    day_results_df['electrolyser_kW_4'] = electrolyser_kW_level_4.values[0:48]
    day_results_df['electrolyser_kW_5'] = electrolyser_kW_level_5.values[0:48]
    day_results_df['h2_produced_kWh'] = h2_produced_kWh_result[0:48]
    day_results_df['h2_to_storage_kWh'] = h2_to_storage[0:48]
    day_results_df['h2_in_storage_kWh'] = h2_in_storage[0:48]
    day_results_df['h2_cost'] = cost_array[0:48]
    day_results_df['h2_cost_corrected'] = corrected_cost_array[0:48]

    return(day_results_df, solver_time)

def NoStorageDay(date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh):

    day_results_df['datetime'] = date_array[0:48]
    day_results_df['price'] = price_array[0:48]
    day_results_df['h2_demand_kWh'] = demand_array[0:48]
    day_results_df['h2_produced_kWh'] = demand_array[0:48]
    day_results_df['h2_to_storage_kWh'] = 0
    day_results_df['h2_in_storage_kWh'] = day_start_h2_in_storage_kwh
    day_results_df['h2_cost'] = day_results_df['h2_produced_kWh'] * h2_price_array[0:48]

    return(day_results_df)

def BasicControlDay(date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, min_h2_production_kwh, max_h2_production_kwh, max_h2_to_storage_kwh, max_storage_kwh, min_storage_kwh):

    #todo rewrite, first define some varibles: 1. Does the price indicate charge or not? 2. Does future demand indicate a necessary charge? 3. Do we have enough charge in the tank to meet the demand alone?

    future_hours_price = 6
    future_hours_demand = 6
    h2_to_storage = np.zeros(48)
    h2_produced_kWh = np.zeros(48)
    h2_in_storage_kwh = np.zeros(48)

    for i in range(0,48):

        h2_in_storage_kwh[i] = day_start_h2_in_storage_kwh
        useable_h2_in_storage = max(0, h2_in_storage_kwh[i] - min_storage_kwh)
        h2_storage_space_available_kwh = max_storage_kwh - h2_in_storage_kwh[i]

        future_median_price = np.median(price_array[i+1: i+1+future_hours_price*2])
        #future_over_demand = np.clip((demand_array[i + 1: i + 1 + future_hours_demand * 2] - max_h2_production_kwh), 0, None).sum()

        if price_array[i] < future_median_price:
            if (demand_array[i] + h2_storage_space_available_kwh) >= min_h2_production_kwh:
                h2_produced_for_demand = min(demand_array[i], max_h2_production_kwh)
                h2_to_storage[i] = min(h2_storage_space_available_kwh, max_h2_to_storage_kwh, (max_h2_production_kwh - h2_produced_for_demand))
                h2_produced_kWh[i] = h2_produced_for_demand + max(0, h2_to_storage[i])
            elif h2_in_storage_kwh[i] >= demand_array[i]:
                h2_to_storage[i] = -demand_array[i]
                h2_produced_kWh[i] = 0
            else:
                raise Exception('Cannot Meet Contraints')

        # elif future_over_demand > useable_h2_in_storage:
        #     if (demand_array[i] + future_over_demand - useable_h2_in_storage) >= min_h2_production_kwh:
        #         h2_produced_for_demand = min(demand_array[i], max_h2_production_kwh)
        #         h2_to_storage[i] = min(future_over_demand - useable_h2_in_storage, max_h2_to_storage_kwh, (max_h2_production_kwh - h2_produced_for_demand))
        #         h2_produced_kWh[i] = h2_produced_for_demand + h2_to_storage[i]
        #     elif (demand_array[i] + h2_storage_space_available_kwh) >= min_h2_production_kwh:
        #         h2_produced_kWh[i] = min_h2_production_kwh
        #         h2_to_storage[i] = min_h2_production_kwh - demand_array[i]
        #     elif h2_in_storage_kwh[i] >= demand_array[i]:
        #         h2_to_storage[i] = -demand_array[i]
        #         h2_produced_kWh[i] = 0
        #     else:
        #         raise Exception('Cannot Meet Contraints')

        elif useable_h2_in_storage >= demand_array[i]:
            h2_to_storage[i] = -demand_array[i]
            h2_produced_kWh[i] = 0
        else:
            h2_to_storage[i] = - useable_h2_in_storage
            h2_produced_kWh[i] = max(min(demand_array[i] - useable_h2_in_storage, max_h2_production_kwh), min_h2_production_kwh)

            if h2_produced_kWh[i] + useable_h2_in_storage >= demand_array[i]:
                h2_to_storage[i] = h2_produced_kWh[i] - demand_array[i]
            else:
                h2_to_storage[i] = 0
                print(date_array[i], 'Cannot meet demand for this period!')

        h2_in_storage_kwh[i] += h2_to_storage[i]
        day_start_h2_in_storage_kwh = h2_in_storage_kwh[i]


    day_results_df['datetime'] = date_array[0:48]
    day_results_df['price'] = price_array[0:48]
    day_results_df['h2_demand_kWh'] = demand_array[0:48]
    day_results_df['h2_produced_kWh'] = h2_produced_kWh[0:48]
    day_results_df['h2_to_storage_kWh'] = h2_to_storage[0:48]
    day_results_df['h2_in_storage_kWh'] = h2_in_storage_kwh[0:48]
    day_results_df['h2_cost'] = day_results_df['h2_produced_kWh'] * h2_price_array[0:48]

    return(day_results_df)

