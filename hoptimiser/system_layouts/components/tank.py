import numpy as np


class HydrogenTank:
    def __init__(self, oem: str, unit_capacity_mwh_h2: float, unit_rated_charge_rate_mw_h2: float,
                 unit_rated_discharge_rate_mw_h2: float, leakage_rate_pct_per_day: float,
                 unit_capex: float, unit_opex_per_year: float,
                 unit_basic_floor_area: float, unit_buffer_distance: float, unit_floor_area: float,
                 rated_pressure: float, count: int, heating_value: str, initial_soc: float = 0.0):

        self._validate_inputs(unit_rated_charge_rate_mw_h2, unit_rated_discharge_rate_mw_h2)

        self._oem = oem
        self._unit_capacity_mwh_h2 = unit_capacity_mwh_h2

        # rated_charge_rate_mw_h2 is a negative value and unit_rated_discharge_rate_mw_h2 is positive.
        # unit_rated_charge_rate is based on total charge rate of all compressors divided by the number of tanks.
        self._unit_rated_charge_rate_mw_h2 = unit_rated_charge_rate_mw_h2
        # unit_rated_discharge_rate_mw_h2 is typically set to infinity.
        self._unit_rated_discharge_rate_mw_h2 = unit_rated_discharge_rate_mw_h2

        self._leakage_rate_pct_per_day = leakage_rate_pct_per_day
        self._unit_capex = unit_capex
        self._unit_opex_per_year = unit_opex_per_year
        self._unit_basic_floor_area = unit_basic_floor_area
        self._unit_buffer_distance = unit_buffer_distance
        self._unit_floor_area = unit_floor_area
        self._rated_pressure = rated_pressure
        self._count = count
        self._heating_value = heating_value
        self._initial_soc = initial_soc

        self._capacity_mwh_h2 = self._unit_capacity_mwh_h2 * self._count

        self._rated_charge_rate_mw_h2 = self._unit_rated_charge_rate_mw_h2 * self._count
        self._rated_discharge_rate_mw_h2 = self._unit_rated_discharge_rate_mw_h2 * self._count
        self._capex = self._unit_capex * self._count
        self._opex_per_year = self._unit_opex_per_year * self._count
        self._floor_area = self._unit_floor_area * self._count

        self._charge_mwh_h2 = 0.0
        self._discharge_mwh_h2 = 0.0
        self._leakage_mwh_h2 = 0.0
        self._stored_mwh_h2 = self._capacity_mwh_h2 * self._initial_soc

    def _validate_inputs(self, unit_rated_charge_rate_mw_h2: float, unit_rated_discharge_rate_mw_h2: float):
        # TODO: Add validation for initialization inputs.

        if not isinstance(unit_rated_charge_rate_mw_h2, (float, int)):
            raise TypeError(
                f'Tank unit rated charge rate ({unit_rated_charge_rate_mw_h2} MW-H2) must be a number less than or equal to zero.')
        if unit_rated_charge_rate_mw_h2 > 0:
            raise ValueError(
                f'Tank unit rated charge rate ({unit_rated_charge_rate_mw_h2} MW-H2) must be a number less than or equal to zero.')

        if not isinstance(unit_rated_discharge_rate_mw_h2, (float, int)):
            raise TypeError(
                f'Tank unit rated discharge rate ({unit_rated_discharge_rate_mw_h2} MW-H2) must be a number greater than or equal to zero.')
        if unit_rated_discharge_rate_mw_h2 < 0:
            raise ValueError(
                f'Tank unit rated discharge rate ({unit_rated_discharge_rate_mw_h2} MW-H2) must be a number greater than or equal to zero.')

    def _get_hours(self, seconds: float) -> float:
        return seconds / 3600.0

    def _get_possible_charge_rate(self, seconds: float) -> float:
        hours = self._get_hours(seconds=seconds)
        leakage_mwh_h2 = self._get_leakage(seconds=seconds)
        unused_capacity_mwh_h2 = self._capacity_mwh_h2 - self._stored_mwh_h2 + leakage_mwh_h2
        # NOTE: self._rated_charge_rate_mw_h2 is a negative value
        charge_rate_mw_h2 = max(-unused_capacity_mwh_h2 / hours, self._rated_charge_rate_mw_h2)
        return charge_rate_mw_h2

    def _get_possible_discharge_rate(self, seconds: float) -> float:
        hours = self._get_hours(seconds=seconds)
        leakage_mwh_h2 = self._get_leakage(seconds=seconds)
        available_stored_mwh_h2 = self._stored_mwh_h2 - leakage_mwh_h2
        discharge_rate_mw_h2 = min(available_stored_mwh_h2 / hours, self._rated_discharge_rate_mw_h2)
        return discharge_rate_mw_h2

    def _validate_charge_rate(self, charge_rate_mw_h2: float, seconds: float) -> float:
        possible_charge_rate_mw_h2 = self._get_possible_charge_rate(seconds=seconds)
        if np.isclose(charge_rate_mw_h2, possible_charge_rate_mw_h2):
            charge_rate_mw_h2 = possible_charge_rate_mw_h2

        if charge_rate_mw_h2 is None:
            raise ValueError('Requested hydrogen storage charge rate is None.')
        elif charge_rate_mw_h2 < possible_charge_rate_mw_h2:
            raise ValueError(
                f'Requested hydrogen storage charge rate {charge_rate_mw_h2} is less than possible charge rate ({possible_charge_rate_mw_h2} MW-H2), '
                f'i.e., attempting to charge more H2 than possible.')
        elif charge_rate_mw_h2 > 0:
            raise ValueError(
                f'Hydrogen storage charge rate ({charge_rate_mw_h2} MW-H2) must be less than or equal to zero.')
        else:
            return charge_rate_mw_h2

    def _validate_discharge_rate(self, discharge_rate_mw_h2: float, seconds: float) -> float:
        possible_discharge_rate_mw_h2 = self._get_possible_discharge_rate(seconds=seconds)
        if np.isclose(discharge_rate_mw_h2, possible_discharge_rate_mw_h2):
            discharge_rate_mw_h2 = possible_discharge_rate_mw_h2

        if discharge_rate_mw_h2 is None:
            raise ValueError('Requested hydrogen storage discharge rate is None.')
        elif discharge_rate_mw_h2 > possible_discharge_rate_mw_h2:
            raise ValueError(
                f'Requested hydrogen storage discharge rate ({discharge_rate_mw_h2} MW-H2) exceeds possible discharge rate ({possible_discharge_rate_mw_h2} MW-H2).')
        elif discharge_rate_mw_h2 < 0:
            raise ValueError(
                f'Requested hydrogen storage discharge rate ({discharge_rate_mw_h2} MW-H2) must be greater than zero.')
        else:
            return discharge_rate_mw_h2

    def _get_leakage(self, seconds: float) -> float:
        n = 24.0 * 3600.0 / seconds
        leakage_rate_per_n = 1 - (1 - 1e-2 * self._leakage_rate_pct_per_day)**(1 / n)
        leakage_mwh_h2 = self._stored_mwh_h2 * leakage_rate_per_n
        return leakage_mwh_h2

    def _charge(self, charge_rate_mw_h2: float, seconds: float):
        hours = self._get_hours(seconds=seconds)
        self._discharge_mwh_h2 = 0.0
        charge_rate_mw_h2 = self._validate_charge_rate(charge_rate_mw_h2=charge_rate_mw_h2, seconds=seconds)
        self._charge_mwh_h2 = charge_rate_mw_h2 * hours
        self._stored_mwh_h2 -= self._charge_mwh_h2
        self._leakage_mwh_h2 = self._get_leakage(seconds=seconds)
        self._stored_mwh_h2 -= self._leakage_mwh_h2

    def _discharge(self, discharge_rate_mw_h2: float, seconds: float):
        hours = self._get_hours(seconds=seconds)
        self._charge_mwh_h2 = 0.0
        discharge_rate_mw_h2 = self._validate_discharge_rate(discharge_rate_mw_h2=discharge_rate_mw_h2, seconds=seconds)
        self._discharge_mwh_h2 = discharge_rate_mw_h2 * hours
        self._stored_mwh_h2 -= self._discharge_mwh_h2
        self._leakage_mwh_h2 = self._get_leakage(seconds=seconds)
        self._stored_mwh_h2 -= self._leakage_mwh_h2

    def _get_discharge_power(self, discharge_rate_mw_h2: float) -> float:
        # It's currently assumed that power is not required to discharge tanks.
        return 0.0

    @property
    def oem(self):
        return self._oem

    @property
    def unit_capacity_mwh_h2(self):
        return self._unit_capacity_mwh_h2

    @property
    def unit_rated_charge_rate_mw_h2(self):
        return self._unit_rated_charge_rate_mw_h2

    @property
    def unit_rated_discharge_rate_mw_h2(self):
        return self._unit_rated_discharge_rate_mw_h2

    @property
    def leakage_rate_pct_per_day(self):
        return self._leakage_rate_pct_per_day

    @property
    def unit_capex(self):
        return self._unit_capex

    @property
    def unit_opex_per_year(self):
        return self._unit_opex_per_year

    @property
    def unit_basic_floor_area(self):
        return self._unit_basic_floor_area

    @property
    def unit_buffer_distance(self):
        return self._unit_buffer_distance

    @property
    def unit_floor_area(self):
        return self._unit_floor_area

    @property
    def rated_pressure(self):
        return self._rated_pressure

    @property
    def count(self):
        return self._count

    @property
    def heating_value(self):
        return self._heating_value

    @property
    def capacity_mwh_h2(self):
        return self._capacity_mwh_h2

    @property
    def rated_charge_rate_mw_h2(self):
        return self._rated_charge_rate_mw_h2

    @property
    def rated_discharge_rate_mw_h2(self):
        return self._rated_discharge_rate_mw_h2

    @property
    def capex(self):
        return self._capex

    @property
    def opex_per_year(self):
        return self._opex_per_year

    @property
    def floor_area(self):
        return self._floor_area

    @property
    def initial_soc(self):
        return self._initial_soc

    @property
    def charge_mwh_h2(self):
        return self._charge_mwh_h2

    @property
    def discharge_mwh_h2(self):
        return self._discharge_mwh_h2

    @property
    def leakage_mwh_h2(self):
        return self._leakage_mwh_h2

    @property
    def stored_mwh_h2(self):
        return self._stored_mwh_h2
