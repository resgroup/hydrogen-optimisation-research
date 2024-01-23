from typing import Tuple, Dict

import numpy as np
import pandas as pd

from pulp import LpProblem, LpMinimize, LpVariable, value, PULP_CBC_CMD


from hoptimiser.system_layouts.topologies.standalone_electrolzyer_with_storage import SystemLayout


class Controller:
    def __init__(self, system_layout: SystemLayout, kwargs: Dict) -> None:
        self._system_layout = system_layout
        self._kwargs = kwargs
        self.case_count = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0, 6: 0}

    def case_1(self, h2_demand_min_energy: float, h2_demand_max_energy: float,
               tank_possible_charge_kwh_h2: float, tank_possible_discharge_kwh_h2: float,
               electrolyzer_min_h2_yield_energy: float, electrolyzer_max_h2_yield_energy: float,
               offsite_h2_yield_energy: float) -> Tuple[float, float]:
        """
        electrolyzer_max_h2_yield_energy <= h2_demand_min_energy
        NOTE: Tank will never charge in this case.
        """
        if tank_possible_discharge_kwh_h2 < (h2_demand_min_energy - electrolyzer_max_h2_yield_energy):
            raise Exception('The electrolyzer and storage combination can NOT meet the minimum H2 demand.')
        else:
            if (np.isclose(offsite_h2_yield_energy, 0.0, rtol=1e-9, atol=1e-9)
                    and (tank_possible_discharge_kwh_h2 >= h2_demand_min_energy)):
                electrolyzer_h2_yield_energy = 0.0
                # Discharge
                tank_response_kwh_h2 = h2_demand_min_energy
            else:
                electrolyzer_h2_yield_energy = max(h2_demand_min_energy - tank_possible_discharge_kwh_h2,
                                                   max(electrolyzer_min_h2_yield_energy, offsite_h2_yield_energy))
                tank_response_kwh_h2 = min(h2_demand_min_energy - electrolyzer_h2_yield_energy,
                                           tank_possible_discharge_kwh_h2)

        return electrolyzer_h2_yield_energy, tank_response_kwh_h2

    def case_2(self, h2_demand_min_energy: float, h2_demand_max_energy: float,
               tank_possible_charge_kwh_h2: float, tank_possible_discharge_kwh_h2: float,
               electrolyzer_min_h2_yield_energy: float, electrolyzer_max_h2_yield_energy: float,
               offsite_h2_yield_energy: float) -> Tuple[float, float]:
        """
        (electrolyzer_min_h2_yield_energy <= h2_demand_min_energy) and (
            electrolyzer_max_h2_yield_energy <= h2_demand_max_energy)
        """
        if np.isclose(offsite_h2_yield_energy, 0.0, rtol=1e-9, atol=1e-9):
            if (tank_possible_discharge_kwh_h2 >= h2_demand_min_energy):
                electrolyzer_h2_yield_energy = 0.0
                # Discharge
                tank_response_kwh_h2 = h2_demand_min_energy
            else:
                electrolyzer_h2_yield_energy = max(h2_demand_min_energy - tank_possible_discharge_kwh_h2,
                                                   electrolyzer_min_h2_yield_energy)
                # Discharge
                tank_response_kwh_h2 = h2_demand_min_energy - electrolyzer_h2_yield_energy

        elif 0.0 < offsite_h2_yield_energy <= h2_demand_min_energy:
            electrolyzer_h2_yield_energy = max(h2_demand_min_energy - tank_possible_discharge_kwh_h2,
                                               offsite_h2_yield_energy)
            # Discharge
            tank_response_kwh_h2 = h2_demand_min_energy - electrolyzer_h2_yield_energy
        else:
            # NOTE: tank_possible_charge_kwh_h2 is a negative value
            electrolyzer_h2_yield_energy = offsite_h2_yield_energy
            # Charge
            tank_response_kwh_h2 = -min(electrolyzer_h2_yield_energy - h2_demand_min_energy,
                                        -tank_possible_charge_kwh_h2)

        return electrolyzer_h2_yield_energy, tank_response_kwh_h2

    def case_3(self, h2_demand_min_energy: float, h2_demand_max_energy: float,
               tank_possible_charge_kwh_h2: float, tank_possible_discharge_kwh_h2: float,
               electrolyzer_min_h2_yield_energy: float, electrolyzer_max_h2_yield_energy: float,
               offsite_h2_yield_energy: float) -> Tuple[float, float]:
        """
        (electrolyzer_min_h2_yield_energy <= h2_demand_min_energy) and (
            electrolyzer_max_h2_yield_energy > h2_demand_max_energy)
        """
        if np.isclose(offsite_h2_yield_energy, 0.0, rtol=1e-9, atol=1e-9):
            if (tank_possible_discharge_kwh_h2 >= h2_demand_min_energy):
                electrolyzer_h2_yield_energy = 0.0
                # Discharge
                tank_response_kwh_h2 = h2_demand_min_energy
            else:
                electrolyzer_h2_yield_energy = max(h2_demand_min_energy - tank_possible_discharge_kwh_h2,
                                                   electrolyzer_min_h2_yield_energy)
                # Discharge
                tank_response_kwh_h2 = h2_demand_min_energy - electrolyzer_h2_yield_energy

        elif 0.0 < offsite_h2_yield_energy <= h2_demand_min_energy:
            electrolyzer_h2_yield_energy = max(h2_demand_min_energy - tank_possible_discharge_kwh_h2,
                                               offsite_h2_yield_energy)
            # Discharge
            tank_response_kwh_h2 = h2_demand_min_energy - electrolyzer_h2_yield_energy
        else:
            # NOTE: tank_possible_charge_kwh_h2 is a negative value
            electrolyzer_h2_yield_energy = min(h2_demand_max_energy - tank_possible_charge_kwh_h2,
                                               offsite_h2_yield_energy)
            # Charge
            tank_response_kwh_h2 = -min(electrolyzer_h2_yield_energy - h2_demand_min_energy,
                                        -tank_possible_charge_kwh_h2)

        return electrolyzer_h2_yield_energy, tank_response_kwh_h2

    def case_4(self, h2_demand_min_energy: float, h2_demand_max_energy: float,
               tank_possible_charge_kwh_h2: float, tank_possible_discharge_kwh_h2: float,
               electrolyzer_min_h2_yield_energy: float, electrolyzer_max_h2_yield_energy: float,
               offsite_h2_yield_energy: float) -> Tuple[float, float]:
        """
        (electrolyzer_min_h2_yield_energy > h2_demand_min_energy) and (
            electrolyzer_max_h2_yield_energy <= h2_demand_max_energy)
        """
        if np.isclose(offsite_h2_yield_energy, 0.0, rtol=1e-9, atol=1e-9) and (tank_possible_discharge_kwh_h2 >= h2_demand_min_energy):
            electrolyzer_h2_yield_energy = 0.0
            # Discharge
            tank_response_kwh_h2 = h2_demand_min_energy
        else:
            # NOTE: tank_possible_charge_kwh_h2 is a negative value
            electrolyzer_h2_yield_energy = max(offsite_h2_yield_energy, electrolyzer_min_h2_yield_energy)
            # Charge
            tank_response_kwh_h2 = -min(electrolyzer_h2_yield_energy - h2_demand_min_energy,
                                        -tank_possible_charge_kwh_h2)
        return electrolyzer_h2_yield_energy, tank_response_kwh_h2

    def case_5(self, h2_demand_min_energy: float, h2_demand_max_energy: float,
               tank_possible_charge_kwh_h2: float, tank_possible_discharge_kwh_h2: float,
               electrolyzer_min_h2_yield_energy: float, electrolyzer_max_h2_yield_energy: float,
               offsite_h2_yield_energy: float) -> Tuple[float, float]:
        """
        (electrolyzer_min_h2_yield_energy > h2_demand_min_energy) and (
            electrolyzer_max_h2_yield_energy > h2_demand_max_energy)
        """
        if np.isclose(offsite_h2_yield_energy, 0.0, rtol=1e-9, atol=1e-9) and (tank_possible_discharge_kwh_h2 >= h2_demand_min_energy):
            electrolyzer_h2_yield_energy = 0.0
            # Discharge
            tank_response_kwh_h2 = h2_demand_min_energy
        elif 0 <= offsite_h2_yield_energy <= h2_demand_min_energy:
            # NOTE: tank_possible_charge_kwh_h2 is a negative value
            electrolyzer_h2_yield_energy = max(offsite_h2_yield_energy, electrolyzer_min_h2_yield_energy)
            # Charge
            tank_response_kwh_h2 = -min(electrolyzer_h2_yield_energy - h2_demand_min_energy,
                                        -tank_possible_charge_kwh_h2)
        else:
            # NOTE: tank_possible_charge_kwh_h2 is a negative value
            electrolyzer_h2_yield_energy = min(h2_demand_max_energy - tank_possible_charge_kwh_h2,
                                               offsite_h2_yield_energy)
            # Charge
            tank_response_kwh_h2 = -min(electrolyzer_h2_yield_energy - h2_demand_min_energy,
                                        -tank_possible_charge_kwh_h2)

        return electrolyzer_h2_yield_energy, tank_response_kwh_h2

    def case_6(self, h2_demand_min_energy: float, h2_demand_max_energy: float,
               tank_possible_charge_kwh_h2: float, tank_possible_discharge_kwh_h2: float,
               electrolyzer_min_h2_yield_energy: float, electrolyzer_max_h2_yield_energy: float,
               offsite_h2_yield_energy: float) -> Tuple[float, float]:
        """
        h2_demand_max_energy <= electrolyzer_min_h2_yield_energy <= electrolyzer_max_h2_yield_energy
        """
        if (offsite_h2_yield_energy > 0) and (-tank_possible_charge_kwh_h2 >= (electrolyzer_min_h2_yield_energy - h2_demand_max_energy)):
            electrolyzer_h2_yield_energy = min(
                offsite_h2_yield_energy, h2_demand_max_energy - tank_possible_charge_kwh_h2)
            # Charge
            tank_response_kwh_h2 = -min(electrolyzer_h2_yield_energy - h2_demand_min_energy,
                                        -tank_possible_charge_kwh_h2)
            # if h2_demand_max_energy == 0:
            #     print(self.idx, 'zero :: charge', offsite_h2_yield_energy, h2_demand_max_energy)

        elif (offsite_h2_yield_energy >= 0) and (tank_possible_discharge_kwh_h2 >= h2_demand_min_energy):
            electrolyzer_h2_yield_energy = 0.0
            # Discharge
            tank_response_kwh_h2 = h2_demand_min_energy
            # if (offsite_h2_yield_energy >= 0) and (h2_demand_max_energy == 0):
            #     print(self.idx, 'zero :: discharge', offsite_h2_yield_energy, h2_demand_max_energy)
        else:
            # raise Exception('The electrolyzer and storage combination can NOT meet the minimum H2 demand.')
            raise Exception(f'Case 6. h2_demand_min_energy: {h2_demand_min_energy}, h2_demand_max_energy: {h2_demand_max_energy}\n'
                            f'electrolyzer_min_h2_yield_energy: {electrolyzer_min_h2_yield_energy}, electrolyzer_max_h2_yield_energy: {electrolyzer_max_h2_yield_energy}\n'
                            f'tank_possible_charge_kwh_h2: {tank_possible_charge_kwh_h2}, tank_possible_discharge_kwh_h2:{tank_possible_discharge_kwh_h2}\n'
                            f'offsite_h2_yield_energy: {offsite_h2_yield_energy}')

        return electrolyzer_h2_yield_energy, tank_response_kwh_h2

    def _request_import_power_at_electrolyzer(self, row: pd.Series) -> Tuple[float, float, float, float]:
        seconds = row['seconds']
        hours = seconds / 3600.0

        # TODO: Limit POI import power.
        poi_offsite_power = -row['PostExportllOffsitePower']
        offsite_power = poi_offsite_power * self._kwargs['hv_trafo_efficiency']

        # Tank charge is a negative value and discharge positive.
        h2_demand_min_energy = row['h2_demand_min']
        h2_demand_max_energy = row['h2_demand_max']

        tank_possible_charge_kwh_h2 = (1e3
                                       * self._system_layout.h2_storage.tank._get_possible_charge_rate(seconds=seconds)
                                       * hours)
        tank_possible_discharge_kwh_h2 = (1e3
                                          * self._system_layout.h2_storage.tank._get_possible_discharge_rate(seconds=seconds)
                                          * hours)

        offsite_h2_yield_energy = self._system_layout.electrolyzer._get_h2_yield_energy(power=offsite_power,
                                                                                        seconds=seconds)

        electrolyzer_min_h2_yield_energy = self._system_layout.electrolyzer.min_h2_yield_power * hours
        electrolyzer_max_h2_yield_energy = self._system_layout.electrolyzer.max_h2_yield_power * hours
        case = None

        if (electrolyzer_min_h2_yield_energy < h2_demand_min_energy) and (electrolyzer_max_h2_yield_energy <= h2_demand_min_energy):
            case = 1
            electrolyzer_h2_yield_energy, tank_response_kwh_h2 = self.case_1(h2_demand_min_energy=h2_demand_min_energy,
                                                                             h2_demand_max_energy=h2_demand_max_energy,
                                                                             tank_possible_charge_kwh_h2=tank_possible_charge_kwh_h2,
                                                                             tank_possible_discharge_kwh_h2=tank_possible_discharge_kwh_h2,
                                                                             electrolyzer_min_h2_yield_energy=electrolyzer_min_h2_yield_energy,
                                                                             electrolyzer_max_h2_yield_energy=electrolyzer_max_h2_yield_energy,
                                                                             offsite_h2_yield_energy=offsite_h2_yield_energy)

        elif (electrolyzer_min_h2_yield_energy <= h2_demand_min_energy) and (h2_demand_min_energy <= electrolyzer_max_h2_yield_energy <= h2_demand_max_energy):
            case = 2
            electrolyzer_h2_yield_energy, tank_response_kwh_h2 = self.case_2(h2_demand_min_energy=h2_demand_min_energy,
                                                                             h2_demand_max_energy=h2_demand_max_energy,
                                                                             tank_possible_charge_kwh_h2=tank_possible_charge_kwh_h2,
                                                                             tank_possible_discharge_kwh_h2=tank_possible_discharge_kwh_h2,
                                                                             electrolyzer_min_h2_yield_energy=electrolyzer_min_h2_yield_energy,
                                                                             electrolyzer_max_h2_yield_energy=electrolyzer_max_h2_yield_energy,
                                                                             offsite_h2_yield_energy=offsite_h2_yield_energy)

        elif (electrolyzer_min_h2_yield_energy <= h2_demand_min_energy) and (electrolyzer_max_h2_yield_energy > h2_demand_max_energy):
            case = 3
            electrolyzer_h2_yield_energy, tank_response_kwh_h2 = self.case_3(h2_demand_min_energy=h2_demand_min_energy,
                                                                             h2_demand_max_energy=h2_demand_max_energy,
                                                                             tank_possible_charge_kwh_h2=tank_possible_charge_kwh_h2,
                                                                             tank_possible_discharge_kwh_h2=tank_possible_discharge_kwh_h2,
                                                                             electrolyzer_min_h2_yield_energy=electrolyzer_min_h2_yield_energy,
                                                                             electrolyzer_max_h2_yield_energy=electrolyzer_max_h2_yield_energy,
                                                                             offsite_h2_yield_energy=offsite_h2_yield_energy)

        elif (electrolyzer_min_h2_yield_energy >= h2_demand_min_energy) and (electrolyzer_max_h2_yield_energy <= h2_demand_max_energy):
            case = 4
            electrolyzer_h2_yield_energy, tank_response_kwh_h2 = self.case_4(h2_demand_min_energy=h2_demand_min_energy,
                                                                             h2_demand_max_energy=h2_demand_max_energy,
                                                                             tank_possible_charge_kwh_h2=tank_possible_charge_kwh_h2,
                                                                             tank_possible_discharge_kwh_h2=tank_possible_discharge_kwh_h2,
                                                                             electrolyzer_min_h2_yield_energy=electrolyzer_min_h2_yield_energy,
                                                                             electrolyzer_max_h2_yield_energy=electrolyzer_max_h2_yield_energy,
                                                                             offsite_h2_yield_energy=offsite_h2_yield_energy)

        elif (h2_demand_min_energy <= electrolyzer_min_h2_yield_energy < h2_demand_max_energy) and (electrolyzer_max_h2_yield_energy > h2_demand_max_energy):
            case = 5
            electrolyzer_h2_yield_energy, tank_response_kwh_h2 = self.case_5(h2_demand_min_energy=h2_demand_min_energy,
                                                                             h2_demand_max_energy=h2_demand_max_energy,
                                                                             tank_possible_charge_kwh_h2=tank_possible_charge_kwh_h2,
                                                                             tank_possible_discharge_kwh_h2=tank_possible_discharge_kwh_h2,
                                                                             electrolyzer_min_h2_yield_energy=electrolyzer_min_h2_yield_energy,
                                                                             electrolyzer_max_h2_yield_energy=electrolyzer_max_h2_yield_energy,
                                                                             offsite_h2_yield_energy=offsite_h2_yield_energy)

        elif electrolyzer_min_h2_yield_energy >= h2_demand_max_energy:
            case = 6
            electrolyzer_h2_yield_energy, tank_response_kwh_h2 = self.case_6(h2_demand_min_energy=h2_demand_min_energy,
                                                                             h2_demand_max_energy=h2_demand_max_energy,
                                                                             tank_possible_charge_kwh_h2=tank_possible_charge_kwh_h2,
                                                                             tank_possible_discharge_kwh_h2=tank_possible_discharge_kwh_h2,
                                                                             electrolyzer_min_h2_yield_energy=electrolyzer_min_h2_yield_energy,
                                                                             electrolyzer_max_h2_yield_energy=electrolyzer_max_h2_yield_energy,
                                                                             offsite_h2_yield_energy=offsite_h2_yield_energy)

        else:
            raise Exception(f'Unknown case. h2_demand_min_energy: {h2_demand_min_energy}, h2_demand_max_energy: {h2_demand_max_energy}, '
                            f'electrolyzer_min_h2_yield_energy: {electrolyzer_min_h2_yield_energy}, electrolyzer_max_h2_yield_energy: {electrolyzer_max_h2_yield_energy}')

        self.case_count[case] += 1

        # if self.idx == 0:
        # if True:
        #     print(
        #         f'Case {case}\n'
        #         f'    idx: {self.idx}\n'
        #         f'    h2_demand_min_energy: {h2_demand_min_energy}, h2_demand_max_energy: {h2_demand_max_energy}\n'
        #         f'    electrolyzer_min_h2_yield_energy: {electrolyzer_min_h2_yield_energy}, electrolyzer_max_h2_yield_energy: {electrolyzer_max_h2_yield_energy}\n'
        #         f'    tank_possible_charge_kwh_h2: {tank_possible_charge_kwh_h2}, tank_possible_discharge_kwh_h2: {tank_possible_discharge_kwh_h2}\n'
        #         f'    offsite_h2_yield_energy: {offsite_h2_yield_energy}\n'
        #         f'    electrolyzer_h2_yield_energy: {electrolyzer_h2_yield_energy}\n'
        #         f'    tank_response_kwh_h2: {tank_response_kwh_h2}\n'
        #         f'    hours: {hours}'
        #         f'    tank_response_mw_h2:{1e-3*tank_response_kwh_h2/hours}\n'
        #         f'    {offsite_h2_yield_energy <= electrolyzer_max_h2_yield_energy}'
        #     )

        electrolyzer_power = self._system_layout.electrolyzer._get_power(h2_yield_energy=electrolyzer_h2_yield_energy,
                                                                         seconds=seconds)
        self._system_layout.h2_storage.dispatch_h2_storage(
            energy_flow_mw_h2=1e-3 * tank_response_kwh_h2 / hours, seconds=seconds)

        self._system_layout.h2_to_demand = electrolyzer_h2_yield_energy + tank_response_kwh_h2
        # system_layout.h2_demand_min = h2_demand_min_energy
        # system_layout.h2_demand_max = h2_demand_max_energy
        # system_layout.post_export_ll_offsite_power = poi_offsite_power

        h2_to_demand = electrolyzer_h2_yield_energy + tank_response_kwh_h2
        # dispatch_instruction = system_layout.request_import_power_at_electrolyzer(
        #     electrolyzer_power, dispatch_instruction)
        # self.idx += 1
        # return system_layout.action_dispatch_instruction(dispatch_instruction)

        return electrolyzer_power, electrolyzer_h2_yield_energy, tank_response_kwh_h2, h2_to_demand

    @property
    def system_layout(self):
        return self._system_layout


class LPController:
    def __init__(self, system_layout: SystemLayout, kwargs: Dict) -> None:
        self._system_layout = system_layout
        self._kwargs = kwargs

    def lp(self, h2_demand_min_energy: float, h2_demand_max_energy: float,
           tank_possible_charge_kwh_h2: float, tank_possible_discharge_kwh_h2: float,
           electrolyzer_min_h2_yield_energy: float, electrolyzer_max_h2_yield_energy: float,
           offsite_h2_yield_energy_max: float):

        model = LpProblem("Minimize_grid_import", LpMinimize)
        offsite_h2_yield_energy = LpVariable('offsite_h2_yield_energy', 0.0, offsite_h2_yield_energy_max)
        grid_import_energy = LpVariable('grid_import_energy', 0.0, h2_demand_min_energy)
        storage_response_energy = LpVariable('storage_response_energy',
                                             tank_possible_charge_kwh_h2,
                                             tank_possible_discharge_kwh_h2)
        h2_demand_energy = LpVariable('h2_demand_energy', h2_demand_min_energy, h2_demand_max_energy)

        # Objective function to minimize
        model += offsite_h2_yield_energy + storage_response_energy + grid_import_energy - h2_demand_energy

        # Constraints
        model += offsite_h2_yield_energy + storage_response_energy + grid_import_energy - h2_demand_energy >= 0
        model += offsite_h2_yield_energy + grid_import_energy >= electrolyzer_min_h2_yield_energy
        model += offsite_h2_yield_energy + grid_import_energy <= electrolyzer_max_h2_yield_energy

        if h2_demand_min_energy >= offsite_h2_yield_energy_max:
            # Discharge (positive energy)
            model += storage_response_energy == min(h2_demand_min_energy - offsite_h2_yield_energy_max,
                                                    tank_possible_discharge_kwh_h2)
        else:
            # Charge (negative energy)
            model += storage_response_energy == max(h2_demand_min_energy - offsite_h2_yield_energy_max,
                                                    tank_possible_charge_kwh_h2)

        # Solve
        # model.solve(PULP_CBC_CMD(logPath=r'pulp.log'))
        model.solve(PULP_CBC_CMD(msg=False, keepFiles=False))
        results = {v.name: v.varValue for v in model.variables()}
        results['objective_value'] = value(model.objective)
        # print('    ', results)
        return results

    def _request_import_power_at_electrolyzer(self, row: pd.Series) -> Tuple[float, float, float, float]:
        seconds = row['seconds']
        hours = seconds / 3600.0
        # print(row.name)
        # TODO: Limit POI import power.
        poi_offsite_power = -row['PostExportllOffsitePower']
        offsite_power_max = poi_offsite_power * self._kwargs['hv_trafo_efficiency']

        # Tank charge is a negative value and discharge positive.
        h2_demand_min_energy = row['h2_demand_min']
        h2_demand_max_energy = row['h2_demand_max']

        tank_possible_charge_kwh_h2 = (1e3
                                       * self._system_layout.h2_storage.tank._get_possible_charge_rate(seconds=seconds)
                                       * hours)
        tank_possible_discharge_kwh_h2 = (1e3
                                          * self._system_layout.h2_storage.tank._get_possible_discharge_rate(seconds=seconds)
                                          * hours)

        offsite_h2_yield_energy_max = self._system_layout.electrolyzer._get_h2_yield_energy(power=offsite_power_max,
                                                                                            seconds=seconds)

        electrolyzer_min_h2_yield_energy = self._system_layout.electrolyzer.min_h2_yield_power * hours
        electrolyzer_max_h2_yield_energy = self._system_layout.electrolyzer.max_h2_yield_power * hours

        results = self.lp(h2_demand_min_energy=h2_demand_min_energy,
                          h2_demand_max_energy=h2_demand_max_energy,
                          tank_possible_charge_kwh_h2=tank_possible_charge_kwh_h2,
                          tank_possible_discharge_kwh_h2=tank_possible_discharge_kwh_h2,
                          electrolyzer_min_h2_yield_energy=electrolyzer_min_h2_yield_energy,
                          electrolyzer_max_h2_yield_energy=electrolyzer_max_h2_yield_energy,
                          offsite_h2_yield_energy_max=offsite_h2_yield_energy_max)

        electrolyzer_h2_yield_energy = results['offsite_h2_yield_energy'] + results['grid_import_energy']

        electrolyzer_power = self._system_layout.electrolyzer._get_power(h2_yield_energy=electrolyzer_h2_yield_energy,
                                                                         seconds=seconds)

        tank_response_kwh_h2 = results['storage_response_energy']
        self._system_layout.h2_storage.dispatch_h2_storage(
            energy_flow_mw_h2=1e-3 * tank_response_kwh_h2 / hours, seconds=seconds)

        self._system_layout.h2_to_demand = electrolyzer_h2_yield_energy + tank_response_kwh_h2

        # h2_to_demand = electrolyzer_h2_yield_energy + tank_response_kwh_h2

        h2_to_demand = results['h2_demand_energy']

        return electrolyzer_power, electrolyzer_h2_yield_energy, tank_response_kwh_h2, h2_to_demand

    @property
    def system_layout(self):
        return self._system_layout
