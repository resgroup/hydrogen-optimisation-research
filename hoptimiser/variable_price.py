import numpy as np
import pandas as pd
import datetime

from pulp import COIN, LpProblem, LpMinimize, LpVariable, value, PULP_CBC_CMD


input_file_name = r'C:\Users\tyoung\Documents\GitHub\hydrogen-optimisation-research\examples\Example_demand.csv'

data = pd.read_csv(input_file_name)
data.Time = pd.to_datetime(data.Time, dayfirst=True)
data['Hour'] = data.Time.dt.hour
data['Day'] = data.Time.dt.date

starting_storage_kwh = 5000
max_storage_kwh = 50000
storage_max_charge_rate_kw = 2000

electrolyser_max_power = 10000
electrolyser_min_power = 1000 #todo work out how to include this
electrolyser_efficiency = 0.70

day = data['Day'][0]
data_day = data.loc[((data['Day'] == day) & (data['Hour'] >= 11)) | (data['Day'] == day + datetime.timedelta(days=1)), :]

price_array = data_day.Price
demand_array = data_day.Demand

max_h2_production_kwh = electrolyser_max_power * electrolyser_efficiency * 0.5
max_h2_to_storage_kwh = min(storage_max_charge_rate_kw * 0.5, max_h2_production_kwh)
max_h2_from_storage_kwh = min(max_storage_kwh * 0.5, max(demand_array))

h2_produced_kWh = LpVariable('h2_produced_kWh', np.zeros(len(data_day)), np.repeat(max_h2_production_kwh, len(data_day)))
h2_to_storage = LpVariable('h2_to_storage', np.zeros(len(data_day)), np.repeat(max_h2_to_storage_kwh, len(data_day)))
h2_from_storage = LpVariable('h2_from_storage', np.zeros(len(data_day)), np.repeat(max_h2_from_storage_kwh, len(data_day)))