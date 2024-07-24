import pandas as pd
import numpy as np

def read_ts_data(input_demand_profiles, input_price_profiles, input_file_name_components):

    demand_data = pd.read_csv(input_demand_profiles)
    demand_data.Time = pd.to_datetime(demand_data.Time, dayfirst=True)
    demand_data['Hour'] = demand_data.Time.dt.hour
    demand_data['Day'] = demand_data.Time.dt.date

    price_data = pd.read_csv(input_price_profiles)
    price_data.Time = pd.to_datetime(demand_data.Time, dayfirst=True)
    price_data['Hour'] = demand_data.Time.dt.hour
    price_data['Day'] = demand_data.Time.dt.date

    data = demand_data.merge(price_data)

    use_of_system_table = pd.read_excel(input_file_name_components, sheet_name='UseOfSystemTotal')

    use_of_system_table = pd.melt(use_of_system_table, id_vars=['month', 'weekday'])

    data['month'] = data.Time.dt.month
    data['weekday'] = data.Time.dt.dayofweek
    data['variable'] = data.Time.dt.time

    data = pd.DataFrame.merge(data, use_of_system_table).sort_values('Time').reset_index()


    data = data.rename(columns={"value": "uos_charge"}).drop(columns=['variable', 'month', 'weekday', 'Hour'])


    return data