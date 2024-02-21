import numpy as np
import pandas as pd
import datetime

from pulp import pulp, COIN, LpProblem, LpMinimize, LpVariable, value, PULP_CBC_CMD

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

start_time = datetime.datetime.now()

input_file_name = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\examples\Example_demand.csv'

data = pd.read_csv(input_file_name)
data.Time = pd.to_datetime(data.Time, dayfirst=True)
data['Hour'] = data.Time.dt.hour
data['Day'] = data.Time.dt.date

starting_storage_kwh = 0#5000
max_storage_kwh = 30000
storage_max_charge_rate_kw = 5000

electrolyser_max_power = 20000
electrolyser_min_power = 3000
electrolyser_efficiency = 0.70

electrolyser_min_power = max(electrolyser_min_power, 1E-4)

max_h2_production_kwh = electrolyser_max_power * electrolyser_efficiency * 0.5
min_h2_production_kwh = electrolyser_min_power * electrolyser_efficiency * 0.5
max_h2_to_storage_kwh = min(storage_max_charge_rate_kw * 0.5, max_h2_production_kwh)

total_cost = 0
day_start_h2_in_storage_kwh = starting_storage_kwh

results_df = pd.DataFrame(columns=['price','h2_demand_kWh','h2_produced_kWh','h2_to_storage_kWh','h2_in_storage_kWh'])

day_results_df = results_df.copy()

for day in data['Day'].unique()[0:len(data['Day'].unique())-1]:

    h2_in_storage_kwh = day_start_h2_in_storage_kwh

    data_day = data.loc[((data['Day'] == day) & (data['Hour'] >= 11)) | (data['Day'] == day + datetime.timedelta(days=1)), :]

    #data_following_day_repeat = data.loc[(data['Day'] == day + datetime.timedelta(days=1)), :]
    #data_day = pd.concat([data_day, data_following_day_repeat])

    data_day = data_day.reset_index()

    price_array = data_day.Price
    demand_array = data_day.Demand

    max_h2_from_storage_kwh = min(max_storage_kwh * 0.5, max(demand_array))

    h2_produced_kWh = MultiDimensionalLpVariable('h2_produced_kWh', len(data_day), 0, max_h2_production_kwh, "Continuous")
    h2_to_storage = MultiDimensionalLpVariable('h2_to_storage', len(data_day), -max_h2_from_storage_kwh, max_h2_to_storage_kwh, "Continuous")
    electrolyser_turned_on = MultiDimensionalLpVariable('electrolyser_turned_on', len(data_day), 0, 1, cat = "Binary")

    problem = LpProblem("Minimize_energy_cost_while_meeting_demand", LpMinimize)

    h2_price_array = price_array / electrolyser_efficiency

    cost = 0
    demand_shortfall = 0
    storage_overuse = 0

    for i in range(0,len(h2_price_array)):
        cost += h2_produced_kWh[i] * h2_price_array[i]

    problem += cost

    for i in range(0,len(h2_price_array)):

        h2_storage_space_available_kwh = max_storage_kwh - h2_in_storage_kwh

        demand_shortfall = demand_array[i] - h2_produced_kWh[i] + h2_to_storage[i]
        storage_overuse = -h2_to_storage[i] - h2_in_storage_kwh
        storage_overfill = h2_to_storage[i] - h2_storage_space_available_kwh

        h2_in_storage_kwh = h2_in_storage_kwh + h2_to_storage[i]

        problem += demand_shortfall == 0
        problem += storage_overuse <= 0
        problem += storage_overfill <= 0

        problem += h2_produced_kWh[i] <= electrolyser_turned_on[i] * max_h2_production_kwh
        problem += h2_produced_kWh[i] >= electrolyser_turned_on[i] * min_h2_production_kwh

    problem.solve(PULP_CBC_CMD(msg=False, keepFiles=False, timeLimit = 100.0))

    h2_produced_kWh.evaluate()
    h2_to_storage.evaluate()

    this_day_cost = 0
    h2_in_storage_tracker = day_start_h2_in_storage_kwh
    h2_in_storage = np.zeros(48)
    cost_array = np.zeros(48)

    for i in range(0,48):
        cost_array[i] = h2_produced_kWh.values[i] * h2_price_array[i]
        h2_in_storage_tracker = h2_in_storage_tracker + h2_to_storage.values[i]
        h2_in_storage[i] = h2_in_storage_tracker

    day_start_h2_in_storage_kwh = h2_in_storage_tracker

    total_cost += cost_array.sum()

    day_results_df['price'] = price_array[0:48]
    day_results_df['h2_demand_kWh'] = demand_array[0:48]
    day_results_df['h2_produced_kWh'] = h2_produced_kWh.values[0:48]
    day_results_df['h2_to_storage_kWh'] = h2_to_storage.values[0:48]
    day_results_df['h2_in_storage_kWh'] = h2_in_storage[0:48]
    day_results_df['h2_cost'] = cost_array[0:48]

    results_df = pd.concat([results_df, day_results_df])

print(total_cost/1000000)

results_df.to_csv('results.csv')

end_time = datetime.datetime.now()

time_taken = end_time - start_time

print('Time taken = ', time_taken)
