from typing import Dict
import os
import time

import numpy as np
import pandas as pd

from hoptimiser.system_layouts.topologies.standalone_electrolzyer_with_storage import SystemLayout
from examples.standalone_electrolyzer_with_storage.controller import Controller


class Simulator:
    def __init__(self, kwargs: Dict) -> None:
        self._kwargs = kwargs
        self._output_path = self._kwargs['output_path']
        print(self._output_path)
        self._create_directory_if_not_exists(path=self._output_path)
        # self._system_layout = SystemLayout(kwargs=kwargs)
        self._controller = Controller(system_layout=SystemLayout(output_directory='.', kwargs=kwargs), kwargs=kwargs)

        self._df = self._load_csv_data_source()
        # self._df.to_csv('z_test.csv')
        self._results = []

    def _create_directory_if_not_exists(self, path: str):
        if not os.path.exists(path):
            os.makedirs(path)

    def _load_csv_data_source(self):
        df = pd.read_csv(self._kwargs['csv_data_source'])
        df.rename(columns={'DateTime': 'datetime', 'HydrogenDemandMax': 'h2_demand_max'}, inplace=True)

        df[['offsite_spill', 'grid_import', 'poi', 'hydrogen_storage_unit',
            'tank_in_mwh_h2', 'tank_out_mwh_h2', 'tank_leakage_mwh_h2',
            'tank_stored_mwh_h2', 'electrolyzer_unit', 'h2_yield_energy',
            'soh', 'degradation_learning_rate', 'h2_to_demand', 'h2_demand_min']] = np.nan
        df.insert(df.shape[1] - 1, 'h2_demand_max', df.pop('h2_demand_max'))

        seconds = (pd.to_datetime(df['datetime'].shift(-1)) -
                   pd.to_datetime(df['datetime'])) / pd.Timedelta(seconds=1)
        df.insert(2, 'seconds', seconds)
        df.loc[:, 'seconds'].iat[-1] = df['seconds'].iat[-2]
        df.loc[:, 'h2_demand_min'] = df['h2_demand_max'] * self._kwargs['h2_demand_min_fraction']
        return df

    def _calculate_dispatch(self, row: pd.Series):
        (electrolyzer_power, electrolyzer_h2_yield_energy,
         tank_response_kwh_h2, h2_to_demand) = self._controller._request_import_power_at_electrolyzer(row=row)
        hours = row['seconds'] / 3600.0
        row['poi'] = ((electrolyzer_power + self._controller.system_layout.h2_storage.power)
                      / self._kwargs['hv_trafo_efficiency'] * hours * 1e-3)

        row['hydrogen_storage_unit'] = self._controller.system_layout.h2_storage.power * hours * 1e-3
        row['tank_in_mwh_h2'] = self._controller.system_layout.h2_storage.tank.charge_mwh_h2
        row['tank_out_mwh_h2'] = self._controller.system_layout.h2_storage.tank.discharge_mwh_h2
        row['tank_leakage_mwh_h2'] = self._controller.system_layout.h2_storage.tank.leakage_mwh_h2
        row['tank_stored_mwh_h2'] = self._controller.system_layout.h2_storage.tank.stored_mwh_h2
        row['electrolyzer_unit'] = electrolyzer_power * hours * 1e-3
        row['h2_yield_energy'] = electrolyzer_h2_yield_energy * 1e-3
        row['soh'] = self._controller.system_layout.electrolyzer.soh
        row['efficiency_learning_rate'] = self._controller.system_layout.electrolyzer._efficiency_learning_rate
        row['h2_to_demand'] = h2_to_demand * 1e-3

        # self._results.append({
        #     'hydrogen_storage_unit': self._controller.system_layout.h2_storage.power,
        #     'tank_in_mwh_h2': self._controller.system_layout.h2_storage.tank.charge_mwh_h2,
        #     'tank_out_mwh_h2': self._controller.system_layout.h2_storage.tank.discharge_mwh_h2,
        #     'tank_leakage_mwh_h2': self._controller.system_layout.h2_storage.tank.leakage_mwh_h2,
        #     'tank_stored_mwh_h2': self._controller.system_layout.h2_storage.tank.stored_mwh_h2,
        #     'electrolyzer_unit': electrolyzer_power,
        #     'h2_yield_energy': electrolyzer_h2_yield_energy,
        #     'soh': self._controller.system_layout.electrolyzer.soh,
        #     'degradation_learning_rate': self._controller.system_layout.electrolyzer._efficiency_learning_rate,
        #     'h2_to_demand': h2_to_demand,
        # })
        return row

    def run(self):
        start_time = time.time()
        self._df = self._df.apply(lambda row: self._calculate_dispatch(row), axis=1)

        # TODO: Add Valuer class
        self._df['grid_import'] = (self._df['PostExportllOffsitePower'] *
                                   self._df['seconds'] / 3600.0 / 1000.0
                                   + self._df['poi'])

        self._df.loc[self._df['grid_import'] > 0, 'grid_import'] = 0

        self._df.to_csv(os.path.join(self._output_path, 'results.csv'))
        print('time: ', time.time() - start_time)
        print(f'case_count: {self._controller.case_count}')


if __name__ == '__main__':
    # NOTE: Setup for Boiler\Coleshill V2 Boiler 2023-03-21 16.13.46\Reports\2333906440
    kwargs = {"output_path": 'examples/standalone_electrolyzer_with_storage/output',
              "csv_data_source": "examples/standalone_electrolyzer_with_storage/data/final/Coleshill V2 Boiler 2023-03-21 16.13.46_2333906440_2017.csv",
              "datetime_column": "DateTime",
              "datetime_format": "%Y-%m-%d %H:%M:%S",
              "commissioning_year": 2026,
              "project_life_years": 25.0,
              "h2_demand_min_fraction": 1.0,
              "poi_import_limit_kw": -30000.0,
              "poi_export_limit_kw": 0.0,
              "heating_value": "lhv",
              "hv_trafo_efficiency": 0.95,
              "compressor_oem": "maximator",
              "compressor_unit_rated_charge_rate_mw_h2": -3.1635,
              "compressor_unit_capex": 1400000.0,
              "compressor_unit_opex_per_year": 1000.0,
              "compressor_unit_floor_area": 15.5,
              "compressor_rated_energy_consumption_mwhe_per_mwh_h2": 0.07902639482,
              "compressor_rated_pressure": 250.0,
              "compressor_count": 1,
              "tank_oem": 'nproxx',
              "tank_unit_capacity_mwh_h2": 10.2459105,
              "tank_unit_rated_discharge_rate_mw_h2": "inf",
              "tank_leakage_rate_pct_per_day": 0.12,
              "tank_unit_capex": 299200.0,
              "tank_unit_opex_per_year": 1000.0,
              "tank_unit_basic_floor_area": 15.4479,
              "tank_unit_buffer_distance": 3.0,
              "tank_unit_floor_area": 48.0301899651412,
              "tank_rated_pressure": 250.0,
              "tank_count": 9,
              "tank_initial_soc": 0.0,
              "stack_oem": "elogen",
              "stack_kind": "pem",
              "stack_rated_power_kw": 8000.0,
              "stack_count": 2,
              "stack_min_power_ratio": 0.0,
              "stack_max_power_ratio": 1.0,
              "stack_efficiency_table": {"power_ratio": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
                                         "efficiency": [0.585979585, 0.585979585, 0.634730266, 0.648641755, 0.652674974, 0.652720857, 0.650830302, 0.647911325, 0.644424874, 0.640627652, 0.636672335]},

              "stack_efficiency_learning_curve": {'year': [2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035, 2036, 2037, 2038, 2039, 2040, 2041, 2042, 2043, 2044, 2045, 2046, 2047, 2048, 2049, 2050, 2051, 2052, 2053, 2054, 2055, 2056, 2057, 2058, 2059, 2060, 2061, 2062, 2063, 2064, 2065, 2066, 2067, 2068, 2069, 2070, 2071, 2072, 2073, 2074, 2075, 2076, 2077, 2078, 2079, 2080, 2081, 2082, 2083, 2084, 2085, 2086, 2087, 2088, 2089, 2090, 2091, 2092, 2093, 2094, 2095, 2096, 2097, 2098, 2099, 2100],
                                                  'rate': [0, 0, 0, 0, 0, 0.054545455, 0, 0, 0, 0, 0.038461538, 0, 0, 0, 0, 0.014, 0, 0, 0, 0, 0.016227181, 0, 0, 0, 0, 0.004123711, 0, 0, 0, 0, 0.00621118, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
              "operational_years": 6,
              "stack_degradation_rate_per_year": -0.022,
              #   "stack_degradation_rate_per_year": 0.0,
              "stack_degradation_years": 0,
              "stack_output_pressure_bar": 30.0,
              "stack_capex": 8666666.667,
              "stack_opex_per_year": 173333.3333,
              "stack_initial_replacement_cost": 2400000.0,
              "stack_replacement_learning_curve": {"year": [2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035, 2036, 2037, 2038, 2039, 2040, 2041, 2042, 2043, 2044, 2045, 2046, 2047, 2048, 2049, 2050, 2051, 2052, 2053, 2054, 2055, 2056, 2057, 2058, 2059, 2060, 2061, 2062, 2063, 2064, 2065, 2066, 2067, 2068, 2069, 2070, 2071, 2072, 2073, 2074, 2075, 2076, 2077, 2078, 2079, 2080, 2081, 2082, 2083, 2084, 2085, 2086, 2087, 2088, 2089, 2090, 2091, 2092, 2093, 2094, 2095, 2096, 2097, 2098, 2099, 2100],
                                                   "rate": [0, 0, 0, 0, 0, -0.33, 0, 0, 0, 0, -0.2, 0, 0, 0, 0, -0.06, 0, 0, 0, 0, -0.07, 0, 0, 0, 0, -0.01, 0, 0, 0, 0, -0.01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]},
              "stack_floor_area": 198.0,
              "stack_efficiency_curve_base_year": 2025,
              "electrolyzer_rebuild_allowed": True,
              "discount_rate": 0.12,
              "inflation_rate": 0.0,
              "sleeving_fee": 0.05,
              "water_cost_per_liter": 0.002,
              "water_liters_per_mwh_h2": 601.0,
              }

    kwargs["tank_unit_rated_charge_rate_mw_h2"] = (kwargs["compressor_unit_rated_charge_rate_mw_h2"]
                                                   * kwargs["compressor_count"]
                                                   / kwargs["tank_count"])

    sim = Simulator(kwargs)
    sim.run()
