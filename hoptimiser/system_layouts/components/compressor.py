
class HydrogenCompressor:
    def __init__(self, oem: str, unit_rated_charge_rate_mw_h2: float,
                 unit_capex: float, unit_opex_per_year: float, unit_floor_area: float,
                 rated_energy_consumption_mwhe_per_mwh_h2: float, rated_pressure: float,
                 count: int, heating_value: str):
        self._validate_inputs(unit_rated_charge_rate_mw_h2)
        self._oem = oem
        self._unit_rated_charge_rate_mw_h2 = unit_rated_charge_rate_mw_h2
        self._unit_capex = unit_capex
        self._unit_opex_per_year = unit_opex_per_year
        self._unit_floor_area = unit_floor_area
        self._rated_energy_consumption_mwhe_per_mwh_h2 = rated_energy_consumption_mwhe_per_mwh_h2
        self._rated_pressure = rated_pressure
        self._count = count
        self._heating_value = heating_value

        self._rated_charge_rate_mw_h2 = self._unit_rated_charge_rate_mw_h2 * self._count
        self._capex = self._unit_capex * self._count
        self._opex_per_year = self._unit_opex_per_year * self._count

    def _validate_inputs(self, unit_rated_charge_rate_mw_h2: float):
        # TODO: Add validation for all initialization inputs.

        if not isinstance(unit_rated_charge_rate_mw_h2, (float, int)):
            raise TypeError(
                f'Compressor unit rated charge rate ({unit_rated_charge_rate_mw_h2} MW-H2) must be a number less than or equal to zero.')
        if unit_rated_charge_rate_mw_h2 > 0:
            raise ValueError(
                f'Compressor unit rated charge rate ({unit_rated_charge_rate_mw_h2} MW-H2) must be a number less than or equal to zero.')

    def _validate_charge_rate(self, charge_rate_mw_h2: float):
        if charge_rate_mw_h2 is None:
            raise ValueError('Requested hydrogen compressor charge rate is None.')
        elif charge_rate_mw_h2 > 0:
            raise ValueError(
                f'Hydrogen compressor charge rate must be less than or equal to zero: {charge_rate_mw_h2}')
        elif charge_rate_mw_h2 < self._rated_charge_rate_mw_h2:
            raise ValueError(f'The magnitude of the requested compressor charge rate ({charge_rate_mw_h2} MW-H2) '
                             f'exceeds the rated charge rate ({self._rated_charge_rate_mw_h2} MW-H2).')
        else:
            pass

    def _get_charge_power(self, charge_rate_mw_h2: float) -> float:
        # Charge rate is a negative value.
        self._validate_charge_rate(charge_rate_mw_h2=charge_rate_mw_h2)
        power_kw = 1e3 * charge_rate_mw_h2 * self._rated_energy_consumption_mwhe_per_mwh_h2
        return power_kw

    @property
    def oem(self):
        return self._oem

    @property
    def unit_rated_charge_rate_mw_h2(self):
        return self._unit_rated_charge_rate_mw_h2

    @property
    def unit_capex(self):
        return self._unit_capex

    @property
    def unit_opex_per_year(self):
        return self._unit_opex_per_year

    @property
    def unit_floor_area(self):
        return self._unit_floor_area

    @property
    def rated_energy_consumption_mwhe_per_mwh_h2(self):
        return self._rated_energy_consumption_mwhe_per_mwh_h2

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
    def rated_charge_rate_mw_h2(self):
        return self._rated_charge_rate_mw_h2

    @property
    def capex(self):
        return self._capex

    @property
    def opex_per_year(self):
        return self._opex_per_year
