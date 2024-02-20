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


input_file_name = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\examples\Example_demand.csv'

data = pd.read_csv(input_file_name)
data.Time = pd.to_datetime(data.Time, dayfirst=True)
data['Hour'] = data.Time.dt.hour
data['Day'] = data.Time.dt.date

starting_storage_kwh = 0#5000
max_storage_kwh = 50000
storage_max_charge_rate_kw = 20000

electrolyser_max_power = 20000
electrolyser_min_power = 1000 #todo work out how to include this
electrolyser_efficiency = 0.70

max_h2_production_kwh = electrolyser_max_power * electrolyser_efficiency * 0.5
max_h2_to_storage_kwh = min(storage_max_charge_rate_kw * 0.5, max_h2_production_kwh)

total_cost = 0
day_start_h2_in_storage_kwh = starting_storage_kwh

for day in data['Day'].unique():

    h2_in_storage_kwh = day_start_h2_in_storage_kwh

    print(day)

    data_day = data.loc[((data['Day'] == day) & (data['Hour'] >= 11)) | (data['Day'] == day + datetime.timedelta(days=1)), :]

    data_day = data_day.reset_index()

    price_array = data_day.Price
    demand_array = data_day.Demand

    max_h2_from_storage_kwh = min(max_storage_kwh * 0.5, max(demand_array))

    h2_produced_kWh = MultiDimensionalLpVariable('h2_produced_kWh', len(data_day), 0, max_h2_production_kwh, "Continuous")
    h2_to_storage = MultiDimensionalLpVariable('h2_to_storage', len(data_day), -max_h2_from_storage_kwh, max_h2_to_storage_kwh, "Continuous")

    problem = LpProblem("Minimize_energy_cost_while_meeting_demand", LpMinimize)

    #objective function

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
        #storage_overfill = h2_to_storage[i] - h2_storage_space_available_kwh

        h2_in_storage_kwh = h2_in_storage_kwh + h2_to_storage[i]

        problem += demand_shortfall == 0
        problem += storage_overuse <= 0
        #problem += storage_overfill <= 0

    problem.solve(PULP_CBC_CMD(msg=True, keepFiles=False, timeLimit = 1.0))

    print(problem.objective.value()/1000000)

    h2_produced_kWh.evaluate()
    h2_to_storage.evaluate()

    this_day_cost = 0
    h2_in_storage_tracker = day_start_h2_in_storage_kwh

    for i in range(0,48):
        this_day_cost += h2_produced_kWh.values[i] * h2_price_array[i]
        h2_in_storage_tracker = h2_in_storage_tracker + h2_to_storage.values[i]

    day_start_h2_in_storage_kwh = h2_in_storage_tracker

    print(day_start_h2_in_storage_kwh)

    total_cost += this_day_cost

    print(h2_produced_kWh.values)
    print(h2_to_storage.values)
    print(h2_produced_kWh.values - h2_to_storage.values)

    #todo we need to extract h2_in_storage at 11am the following day, not at the end of the forecast period. Do this by looping through steps again using the known control logic


print(total_cost)

