from typing import Tuple
import os

import numpy as np
import pandas as pd
from scipy.interpolate import interp1d, PchipInterpolator

from hoptimiser.system_layouts.components.stack import Stack


class ElectrolyzerUnit:
    def __init__(self, stack: Stack, commissioning_year: int, operational_years: int, name: str = 'Electrolyzer Unit',):

        # self._validate_inputs(stack, max_rated_power, degradation_pct_per_hr, rebuild_allowed, lifetime_hours)

        self.name = name
        self._stack = stack
        self._commissioning_year = commissioning_year

        self._rated_power = self._stack.rated_power * self._stack.count
        self._min_rated_power = self._stack.min_power_ratio * self._rated_power
        self._max_rated_power = self._stack.max_power_ratio * self._rated_power

        self._operational_years = operational_years

        self._soh, self._degradation = self._get_soh_and_degradation()

        self._efficiency_learning_curve = self._get_efficiency_learning_curve()
        self._efficiency_learning_rate = self._get_efficiency_learning_rate()

        self._efficiency_lookup_table = self._get_efficiency_lookup_table()
        self._interpolate_h2_yield_power = self._train_h2_yield_power_interpolator()
        self._interpolate_input_power = self._train_input_power_interpolator()

        self._total_h2_yield_energy = 0.0
        self._h2_yield_energy = None

    def _validate_inputs(self):
        raise NotImplementedError('Method _validate_inputs() has not been implemented.')
        pass

    def _get_hours(self, seconds: float) -> float:
        return seconds / 3600.0

    def _get_soh_and_degradation(self) -> Tuple[float, float]:
        # HACK: Subtracting 1 year from self._operational_years.
        # TODO: Confirm subtracting 1 year is not needed.
        soh = (1.0 + self.stack.degradation_rate_per_year)**(self._operational_years - 1.0)
        degrdation = 1.0 - soh
        return soh, degrdation

    def _get_efficiency_learning_curve(self) -> pd.Series:
        df = pd.DataFrame.from_dict(self._stack.efficiency_learning_curve)  # .set_index(keys='year', drop=True)
        df_tmp = df.loc[df['rate'] > 0].reset_index(drop=True)
        df_tmp['n'] = df_tmp['year'].diff()
        df_tmp.loc[0, 'n'] = df_tmp.loc[0, 'year'] - df.loc[0, 'year']

        df = df.merge(df_tmp[['year', 'n']], how='left', on='year')
        df.loc[df['rate'] == 0, 'rate'] = np.nan
        df = df.bfill().dropna()

        # NOTE: Incorrectly calculates yearly efficiency learning as 1-(1 - df['rate'])**(1 / df['n']))
        df['yearly_rate'] = 1 - (1 - df['rate'])**(1 / df['n'])
        # TODO: Confirm logic below is correct relative to above line.
        # df['yearly_rate'] = (1 + df['rate'])**(1 / df['n']) - 1

        df.drop(columns=['n'], inplace=True)

        df = df.loc[df['year'] >= self.stack.efficiency_curve_base_year].reset_index(drop=True)
        df.loc[0, 'yearly_rate'] = 0.0

        df['adjustment_factor'] = (1 + df['yearly_rate']).cumprod()
        df.set_index('year', drop=True, inplace=True)
        for idx, row in df.iterrows():
            print(idx, row['rate'], row['yearly_rate'], row['adjustment_factor'])
        df.to_csv('z_learn_rates.csv')
        return df

    def _get_efficiency_learning_rate(self) -> float:
        min_year = self._efficiency_learning_curve.index.min()
        max_year = self._efficiency_learning_curve.index.max()
        if min_year <= self._commissioning_year <= max_year:
            efficiency_learning_rate = self._efficiency_learning_curve['adjustment_factor'].at[self._commissioning_year]
        else:
            raise ValueError('Unable to update the efficiency learning rate. '
                             f'Commissioning year ({self._commissioning_year}) must be between {min_year} and {max_year}')
        return efficiency_learning_rate

    def _get_efficiency_lookup_table(self) -> pd.DataFrame:
        input_power_train = self._rated_power * np.array(self._stack.efficiency_table['power_ratio'])
        efficiency_train = np.array(
            self._stack.efficiency_table['efficiency']) * self._efficiency_learning_rate * self._soh

        # TODO: Delete print
        print(f'efficency_learning_rate: {self._efficiency_learning_rate}, soh: {self._soh}, deg: {self._degradation}')

        df_tmp = pd.DataFrame(
            data={'power_ratio': self._stack.efficiency_table['power_ratio'], 'efficiency': efficiency_train})
        df_tmp.to_csv('z_efficiency_curve_scaled.csv')

        # Interpolates using a cubic spline over interval input_power[1:]
        get_efficiency = PchipInterpolator(x=input_power_train[1:], y=efficiency_train[1:])

        # Interpolates linearly over interval [0, input_power_train[1]]
        get_efficiency_near_zero_boundary = interp1d(x=input_power_train[:2],
                                                     y=efficiency_train[:2],
                                                     kind='linear',
                                                     bounds_error=True)

        input_power = np.arange(np.floor(self._min_rated_power),
                                np.ceil(self._max_rated_power + 1),
                                1)
        power_ratio = input_power / self._rated_power
        efficiency = np.insert(arr=get_efficiency(input_power[input_power >= input_power_train[1]]),
                               obj=0,
                               values=get_efficiency_near_zero_boundary(input_power[input_power < input_power_train[1]]))
        h2_yield_power = input_power * efficiency

        return pd.DataFrame.from_dict({'power_ratio': power_ratio,
                                       'input_power': -input_power,
                                       'efficiency': efficiency,
                                       'h2_yield_power': h2_yield_power})

    def _train_h2_yield_power_interpolator(self) -> interp1d:
        return interp1d(x=self._efficiency_lookup_table['input_power'],
                        y=self._efficiency_lookup_table['h2_yield_power'],
                        kind='linear',
                        bounds_error=True)

    def _train_input_power_interpolator(self) -> interp1d:
        return interp1d(x=self._efficiency_lookup_table['h2_yield_power'],
                        y=self._efficiency_lookup_table['input_power'],
                        kind='linear',
                        bounds_error=True)

    def _get_h2_yield_energy(self, power: float, seconds: float) -> float:
        # TODO: Validate input power
        power = self._clip_to_rated_powers(power=power)
        if np.isclose(power, 0.0, rtol=1e-9, atol=1e-9):
            return 0
        else:
            hours = self._get_hours(seconds=seconds)
            h2_yield_power = self._interpolate_h2_yield_power(power)
            return h2_yield_power * hours

    def _get_power(self, h2_yield_energy: float, seconds: float) -> float:
        if np.isclose(h2_yield_energy, 0.0, rtol=1e-9, atol=1e-9):
            return 0
        else:
            hours = self._get_hours(seconds=seconds)

            min_h2_yield_energy_net = self.min_h2_yield_power * hours
            max_h2_yield_energy_net = self.max_h2_yield_power * hours

            min_h2_yield_power_gross = self._interpolate_input_power.x[0]
            max_h2_yield_power_gross = self._interpolate_input_power.x[-1]

            h2_yield_power_net = h2_yield_energy / hours
            h2_yield_power_gross = h2_yield_power_net  # / soh / self._efficiency_learning_rate

            # Handle floating point fuzziness
            if np.isclose(h2_yield_power_gross, min_h2_yield_power_gross, rtol=1e-9, atol=1e-9):
                h2_yield_power_gross = min_h2_yield_power_gross
            elif np.isclose(h2_yield_power_gross, max_h2_yield_power_gross, rtol=1e-9, atol=1e-9):
                h2_yield_power_gross = max_h2_yield_power_gross
            elif (h2_yield_power_gross < min_h2_yield_power_gross) or (h2_yield_power_gross > max_h2_yield_power_gross):
                raise ValueError(
                    f'h2_yield_energy {h2_yield_energy} must be between {min_h2_yield_energy_net} and {max_h2_yield_energy_net}')
            else:
                pass

            return self._interpolate_input_power(h2_yield_power_gross)

    def _clip_to_rated_powers(self, power: float) -> float:
        # NOTE: The system layout signs power negatively for loads.
        if power > 0:
            raise ValueError('Electrolyzer power must be less than or equal to zero.')

        available_power = power
        power = max(available_power, -self._max_rated_power)
        if power < -self._min_rated_power:
            pass
        elif np.isclose(power, -self._min_rated_power, rtol=1e-9, atol=1e-9):
            power = -self._min_rated_power
        else:
            power = 0
        return power

    @ property
    def stack(self):
        return self._stack

    @ property
    def rated_power(self):
        return self._rated_power

    @ property
    def max_rated_power(self):
        return self._max_rated_power

    @ property
    def min_rated_power(self):
        return self._min_rated_power

    @ property
    def min_h2_yield_power(self):
        return self._get_h2_yield_energy(power=-self._min_rated_power, seconds=3600.0)

    @ property
    def max_h2_yield_power(self):
        return self._get_h2_yield_energy(power=-self._max_rated_power, seconds=3600.0)

    @ property
    def h2_yield_energy(self):
        return self._h2_yield_energy

    @ property
    def degradation(self):
        return self._degradation

    @ property
    def soh(self):
        return self._soh
