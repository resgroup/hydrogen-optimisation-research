import pandas as pd
import numpy as np

def read_ts_data(input_demand_profiles, input_price_profiles):

    demand_data = pd.read_excel(input_demand_profiles, sheet_name='demand')
    demand_data.Time = pd.to_datetime(demand_data.Time, dayfirst=True)
    demand_data['Hour'] = demand_data.Time.dt.hour
    demand_data['Day'] = demand_data.Time.dt.date

    price_data = pd.read_excel(input_price_profiles, sheet_name='price')
    price_data.Time = pd.to_datetime(demand_data.Time, dayfirst=True)
    price_data['Hour'] = demand_data.Time.dt.hour
    price_data['Day'] = demand_data.Time.dt.date

    data = demand_data.merge(price_data)

    return data