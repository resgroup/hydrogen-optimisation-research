from typing import Dict

import numpy as np

from hoptimiser.system_layouts.components.electrolyzer_unit import Stack, ElectrolyzerUnit
from hoptimiser.system_layouts.components.tank import HydrogenTank
from hoptimiser.system_layouts.components.compressor import HydrogenCompressor
from hoptimiser.system_layouts.components.hydrogen_storage_unit import HydrogenStorageUnit


class SystemLayout:
    def __init__(self, output_directory: str, kwargs: Dict) -> None:
        self._output_directory = output_directory
        self._kwargs = kwargs
        self.poi_import_limit_kw = self._kwargs['poi_import_limit_kw']
        self.poi_export_limit_kw = self._kwargs['poi_export_limit_kw']
        self.hv_trafo_efficiency = self._kwargs['hv_trafo_efficiency']
        self.electrolyzer = self._get_electrolyzer()
        self.h2_storage = self._get_hydrogen_storage_unit()
        self._is_logging = True
        self._is_logging_load_profile = False

    def _get_stack(self):
        return Stack(oem=self._kwargs['stack_oem'],
                     kind=self._kwargs['stack_kind'],
                     rated_power=self._kwargs['stack_rated_power_kw'],
                     count=self._kwargs['stack_count'],
                     min_power_ratio=self._kwargs['stack_min_power_ratio'],
                     max_power_ratio=self._kwargs['stack_max_power_ratio'],
                     efficiency_curve_base_year=self._kwargs['stack_efficiency_curve_base_year'],
                     efficiency_table=self._kwargs['stack_efficiency_table'],
                     efficiency_learning_curve=self._kwargs['stack_efficiency_learning_curve'],
                     degradation_rate_per_year=self._kwargs['stack_degradation_rate_per_year'],
                     output_pressure_bar=self._kwargs['stack_output_pressure_bar'],
                     capex=self._kwargs['stack_capex'],
                     opex_per_year=self._kwargs['stack_opex_per_year'],
                     initial_replacement_cost=self._kwargs['stack_initial_replacement_cost'],
                     replacement_learning_curve=self._kwargs['stack_replacement_learning_curve'],
                     floor_area=self._kwargs['stack_floor_area'])

    def _get_electrolyzer(self):
        return ElectrolyzerUnit(self._get_stack(),
                                # commissioning_datetime=self.commissioning_datetime,

                                commissioning_year=self._kwargs['commissioning_year'],
                                operational_years=self._kwargs['operational_years'],
                                # rebuild_allowed=self._kwargs['electrolyzer_rebuild_allowed'],
                                name='Electrolyzer Unit',
                                # output_directory=self._output_directory,
                                # is_logging=self._is_logging,
                                # create_load_profile_csv=self._is_logging_load_profile)
                                )

    def _get_compressor(self):
        unit_rated_charge_rate_mw_h2 = self._kwargs.get('compressor_unit_rated_charge_rate_mw_h2')
        if isinstance(unit_rated_charge_rate_mw_h2, (int, float)):
            pass
        elif isinstance(unit_rated_charge_rate_mw_h2, str) and unit_rated_charge_rate_mw_h2.lower() == '-inf':
            unit_rated_charge_rate_mw_h2 = np.NINF
        else:
            raise ValueError(f"compressor_unit_rated_charge_rate_mw_h2 ({unit_rated_charge_rate_mw_h2}) "
                             f"must be an int, float, or a str with value '-inf'.")

        return HydrogenCompressor(oem=self._kwargs['compressor_oem'],
                                  unit_rated_charge_rate_mw_h2=unit_rated_charge_rate_mw_h2,
                                  unit_capex=self._kwargs['compressor_unit_capex'],
                                  unit_opex_per_year=self._kwargs['compressor_unit_opex_per_year'],
                                  unit_floor_area=self._kwargs['compressor_unit_floor_area'],
                                  rated_energy_consumption_mwhe_per_mwh_h2=self._kwargs[
                                      'compressor_rated_energy_consumption_mwhe_per_mwh_h2'],
                                  rated_pressure=self._kwargs['compressor_rated_pressure'],
                                  count=self._kwargs['compressor_count'],
                                  heating_value=self._kwargs['heating_value'])

    def _get_tank(self):
        unit_rated_discharge_rate_mw_h2 = self._kwargs.get('tank_unit_rated_discharge_rate_mw_h2')
        if isinstance(unit_rated_discharge_rate_mw_h2, (int, float)):
            pass
        elif isinstance(unit_rated_discharge_rate_mw_h2, str) and unit_rated_discharge_rate_mw_h2.lower() == 'inf':
            unit_rated_discharge_rate_mw_h2 = np.Inf
        else:
            raise ValueError(f"tank_unit_rated_discharge_rate_mw_h2 ({unit_rated_discharge_rate_mw_h2}) "
                             f"must be an int, float, or a str with value 'inf'.")

        return HydrogenTank(oem=self._kwargs['tank_oem'],
                            unit_capacity_mwh_h2=self._kwargs['tank_unit_capacity_mwh_h2'],
                            unit_rated_charge_rate_mw_h2=self._kwargs['tank_unit_rated_charge_rate_mw_h2'],
                            unit_rated_discharge_rate_mw_h2=unit_rated_discharge_rate_mw_h2,
                            leakage_rate_pct_per_day=self._kwargs['tank_leakage_rate_pct_per_day'],
                            unit_capex=self._kwargs['tank_unit_capex'],
                            unit_opex_per_year=self._kwargs['tank_unit_opex_per_year'],
                            unit_basic_floor_area=self._kwargs['tank_unit_basic_floor_area'],
                            unit_buffer_distance=self._kwargs['tank_unit_buffer_distance'],
                            unit_floor_area=self._kwargs['tank_unit_floor_area'],
                            rated_pressure=self._kwargs['tank_rated_pressure'],
                            count=self._kwargs['tank_count'],
                            heating_value=self._kwargs['heating_value'],
                            initial_soc=self._kwargs['tank_initial_soc'])

    def _get_hydrogen_storage_unit(self):
        return HydrogenStorageUnit(tank=self._get_tank(),
                                   compressor=self._get_compressor(),
                                   name='Hydrogen Storage Unit',
                                   output_directory=self._output_directory,
                                   is_logging=False,
                                   create_load_profile_csv=False)
