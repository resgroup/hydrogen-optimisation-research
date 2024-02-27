import numpy as np
from pulp import pulp, LpProblem, LpMinimize, PULP_CBC_CMD
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

def LPcontrol(data_day, date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, min_h2_production_kwh, max_h2_production_kwh, max_h2_from_storage_kwh, max_h2_to_storage_kwh, max_storage_kwh, min_storage_kwh):

    cost = 0
    h2_produced_kWh = MultiDimensionalLpVariable('h2_produced_kWh', len(data_day), 0, max_h2_production_kwh, "Continuous")
    h2_to_storage = MultiDimensionalLpVariable('h2_to_storage', len(data_day), -max_h2_from_storage_kwh, max_h2_to_storage_kwh, "Continuous")
    electrolyser_turned_on = MultiDimensionalLpVariable('electrolyser_turned_on', len(data_day), 0, 1, cat = "Binary")

    h2_in_storage_kwh = day_start_h2_in_storage_kwh

    problem = LpProblem("Minimize_energy_cost_while_meeting_demand", LpMinimize)

    for i in range(0,len(h2_price_array)):
        cost += h2_produced_kWh[i] * h2_price_array[i]

    problem += cost

    for i in range(0,len(h2_price_array)):

        h2_storage_space_available_kwh = max_storage_kwh - h2_in_storage_kwh

        demand_shortfall = demand_array[i] + h2_to_storage[i] - h2_produced_kWh[i]
        storage_remaining = h2_in_storage_kwh + h2_to_storage[i]
        storage_overfill = h2_to_storage[i] - h2_storage_space_available_kwh

        h2_in_storage_kwh = h2_in_storage_kwh + h2_to_storage[i]

        problem += demand_shortfall == 0
        problem += storage_remaining >= min_storage_kwh
        problem += storage_overfill <= 0

        problem += h2_produced_kWh[i] <= electrolyser_turned_on[i] * max_h2_production_kwh
        problem += h2_produced_kWh[i] >= electrolyser_turned_on[i] * min_h2_production_kwh

    problem.solve(PULP_CBC_CMD(msg=False, keepFiles=False, timeLimit = 100.0))

    h2_produced_kWh.evaluate()
    h2_produced_kWh_result = h2_produced_kWh.values
    h2_to_storage.evaluate()
    h2_to_storage_result = h2_to_storage.values

    h2_in_storage_tracker = day_start_h2_in_storage_kwh
    h2_in_storage = np.zeros(48)
    cost_array = np.zeros(48)

    for i in range(0,48):
        cost_array[i] = h2_produced_kWh_result[i] * h2_price_array[i]
        h2_in_storage_tracker = h2_in_storage_tracker + h2_to_storage_result[i]
        h2_in_storage[i] = h2_in_storage_tracker

    day_results_df['datetime'] = date_array[0:48]
    day_results_df['price'] = price_array[0:48]
    day_results_df['h2_demand_kWh'] = demand_array[0:48]
    day_results_df['h2_produced_kWh'] = h2_produced_kWh.values[0:48]
    day_results_df['h2_to_storage_kWh'] = h2_to_storage.values[0:48]
    day_results_df['h2_in_storage_kWh'] = h2_in_storage[0:48]
    day_results_df['h2_cost'] = cost_array[0:48]

    return(day_results_df)

def NoStorageDay(date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh):

    day_results_df['datetime'] = date_array[0:48]
    day_results_df['price'] = price_array[0:48]
    day_results_df['h2_demand_kWh'] = demand_array[0:48]
    day_results_df['h2_produced_kWh'] = demand_array[0:48]
    day_results_df['h2_to_storage_kWh'] = 0
    day_results_df['h2_in_storage_kWh'] = day_start_h2_in_storage_kwh
    day_results_df['h2_cost'] = day_results_df['h2_produced_kWh'] * h2_price_array[0:48]

    return(day_results_df)

def BasicControlDay(date_array, price_array, h2_price_array, demand_array, day_results_df, day_start_h2_in_storage_kwh, min_h2_production_kwh, max_h2_production_kwh, max_h2_from_storage_kwh, max_h2_to_storage_kwh, max_storage_kwh, min_storage_kwh):

    future_hours = 12
    h2_to_storage = np.zeros(48)
    h2_produced_kWh = np.zeros(48)
    h2_in_storage_kwh = np.zeros(48)

    for i in range(0,48):

        h2_in_storage_kwh[i] = day_start_h2_in_storage_kwh
        h2_storage_space_available_kwh = max_storage_kwh - h2_in_storage_kwh[i]

        future_median_price = np.median(price_array[i: i + future_hours*2])

        if (price_array[i] < future_median_price):
            if (demand_array[i] + h2_storage_space_available_kwh) > min_h2_production_kwh:
                h2_produced_for_demand = demand_array[i]
                h2_to_storage[i] = min(h2_storage_space_available_kwh, max_h2_to_storage_kwh)
                h2_produced_kWh[i] = h2_produced_for_demand + h2_to_storage[i]
            elif h2_in_storage_kwh[i] >= demand_array[i]:
                h2_to_storage[i] = -demand_array[i]
                h2_produced_kWh[i] = 0
            else:
                raise Exception('Cannot Meet Contraints')

        elif h2_in_storage_kwh[i] >= demand_array[i]:
            h2_to_storage[i] = -demand_array[i]
            h2_produced_kWh[i] = 0
        else:
            h2_to_storage[i] = - h2_in_storage_kwh[i]
            h2_produced_kWh[i] = max(demand_array[i] - h2_in_storage_kwh[i], min_h2_production_kwh)
            h2_to_storage[i] = demand_array[i] - h2_produced_kWh[i]

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

