import os

from hoptimiser.system_layouts.components.tank import HydrogenTank
from hoptimiser.system_layouts.components.compressor import HydrogenCompressor


class HydrogenStorageUnit:
    def __init__(self, tank: HydrogenTank, compressor: HydrogenCompressor, name='Hydrogen Storage Unit',
                 is_logging=True, create_load_profile_csv=True, output_directory=os.getcwd()):

        self.name = name
        self.create_load_profile_csv = create_load_profile_csv

        self._tank = tank
        self._compressor = compressor

        self._power = 0.0

        self._total_electrical_energy_consumed_kwh = 0.0
        self._total_h2_discharge_mwh_h2 = 0.0
        self._total_h2_charge_mwh_h2 = 0.0

    def _validate_inputs(self, tank: HydrogenTank, compressor: HydrogenCompressor):
        if not isinstance(tank, HydrogenTank):
            raise TypeError('The tank object must be an instance of class HydrogenTank.')

        if not isinstance(compressor, HydrogenCompressor):
            raise TypeError('The compressor object must be an instance of class HydrogenCompressor.')

    def _get_hours(self, seconds: float):
        return seconds / 3600.0

    def _charge(self, charge_rate_mw_h2: float, seconds: float):
        self._tank._charge(charge_rate_mw_h2=charge_rate_mw_h2, seconds=seconds)
        self._power = self._compressor._get_charge_power(charge_rate_mw_h2=charge_rate_mw_h2)

    def _discharge(self, discharge_rate_mw_h2: float, seconds: float):
        self._tank._discharge(discharge_rate_mw_h2=discharge_rate_mw_h2, seconds=seconds)
        # It's currently assumed that power is not required to discharge tanks.
        self._power = self._tank._get_discharge_power(discharge_rate_mw_h2=discharge_rate_mw_h2)

    def dispatch_h2_storage(self, energy_flow_mw_h2, seconds):
        if energy_flow_mw_h2 >= 0:
            self._discharge(discharge_rate_mw_h2=energy_flow_mw_h2, seconds=seconds)
        else:
            self._charge(charge_rate_mw_h2=energy_flow_mw_h2, seconds=seconds)

        self.update_counters(energy_flow_mw_h2, seconds=seconds)

        try:
            assert self._tank.stored_mwh_h2 >= -1.e-9
            assert self._tank.stored_mwh_h2 <= self._tank.capacity_mwh_h2 + 1e-9
        except AssertionError:
            raise AssertionError(f"{self.__class__.__name__} named {self.name} with capacity "
                                 f"{self._tank.capacity_mwh_h2} MWh-H2 has stored hydrogen {self._tank.stored_mwh_h2} MWh-H2 "
                                 f"after dispatching H2 energy flow of {energy_flow_mw_h2} MWh-H2 "
                                 f"for {seconds} seconds.")

    def request_power(self, dispatch_instruction, **kwargs):
        dispatch_instruction.component_power_required_dict.update({self.name: self._power})
        return dispatch_instruction

    def update_counters(self, energy_flow_mw_h2, seconds):
        hours = seconds / 3600
        self._total_electrical_energy_consumed_kwh += self._power * hours
        self._total_h2_discharge_mwh_h2 += energy_flow_mw_h2 * hours if energy_flow_mw_h2 > 0. else 0.
        self._total_h2_charge_mwh_h2 += energy_flow_mw_h2 * hours if energy_flow_mw_h2 < 0. else 0.

    @property
    def power(self):
        return self._power

    @property
    def tank(self):
        return self._tank

    @property
    def compressor(self):
        return self._compressor
