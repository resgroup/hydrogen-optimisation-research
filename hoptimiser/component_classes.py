import pandas as pd


class CombinedElectrolyser():
    def __init__(self, selected_electrolyser, n_electrolysers, stack_replacement_years, first_operational_year, n_years, electrolyser_min_capacity = 0.1):

        self.max_power = selected_electrolyser['Capacity (MW)'] * 1000 * n_electrolysers
        self.efficiency_load_factor = selected_electrolyser['electrolyser_efficiency_load_factors'][0]
        self.efficiency = selected_electrolyser['electrolyser_efficiency'][0]
        self.min_power = max(self.max_power * electrolyser_min_capacity * n_electrolysers, 1E-4)
        self.capex = selected_electrolyser['CAPEX'] * n_electrolysers
        self.opex_per_year = selected_electrolyser['OPEX (£/year)'] * n_electrolysers
        self.floor_space = int(selected_electrolyser['Floor Space (m2)'] * n_electrolysers)
        self.start_stack_replacement_cost = selected_electrolyser['Start Stack Replacement Cost'] * n_electrolysers
        self.efficiency_load_factor_simplified = [0.2, 0.3, 0.6, 0.8, 1.0]
        self._simplify_efficiencies(self.efficiency_load_factor_simplified)
        self.stack_replacement_capex_curve = selected_electrolyser['stack_replacement_capex'][0]
        self.efficiency_degradation = selected_electrolyser['efficiency_degradation'][0]
        self.efficiency_learning_curve = selected_electrolyser['efficiency_learning'][0]
        self.stack_replacement_capex_curve_year = selected_electrolyser['stack_replacement_capex_year'][0]
        self.efficiency_learning_curve_year = selected_electrolyser['efficiency_learning_year'][0]

        self.combined_stack_and_efficiencies_df = self._combine_learning_data(first_operational_year, stack_replacement_years, n_years)

        self.minimum_efficiency = self.combined_stack_and_efficiencies_df['final_relative_efficiency'].min()

    def _combine_learning_data(self, first_operational_year, stack_replacement_years, n_years):

        efficiency_learning_df = pd.DataFrame()
        efficiency_learning_df['Year'] = self.efficiency_learning_curve_year
        efficiency_learning_df['efficiency_learning_curve'] = self.efficiency_learning_curve

        stack_replacement_df = pd.DataFrame()
        stack_replacement_df['Year'] = self.stack_replacement_capex_curve_year
        stack_replacement_df['stack_replacement_capex_curve'] = self.stack_replacement_capex_curve
        stack_replacement_df['stack_replacement'] = False
        stack_replacement_df['cumulative_capex_curve'] = 1 + stack_replacement_df['stack_replacement_capex_curve']

        stack_replacement_df = stack_replacement_df[0:n_years]

        for replacement_year in stack_replacement_years:
            stack_replacement_df.loc[replacement_year-1, 'stack_replacement'] = True

        for y in range(1,len(stack_replacement_df)):
            stack_replacement_df.loc[y, 'cumulative_capex_curve'] = stack_replacement_df.loc[y - 1, 'cumulative_capex_curve'] * stack_replacement_df.loc[y, 'cumulative_capex_curve']

        stack_replacement_df['stack_final_capex'] = stack_replacement_df['cumulative_capex_curve'] * self.start_stack_replacement_cost

        if not stack_replacement_df['Year'][0] == first_operational_year:
            raise Exception('First operational year does not match first year of stack replacement data!')

        combined_stack_and_efficiencies_df = efficiency_learning_df.merge(stack_replacement_df)

        combined_stack_and_efficiencies_df['cumulative_learning_curve'] = 1 + combined_stack_and_efficiencies_df['efficiency_learning_curve']
        for y in range(1, len(combined_stack_and_efficiencies_df)):
            combined_stack_and_efficiencies_df.loc[y, 'cumulative_learning_curve'] = combined_stack_and_efficiencies_df.loc[y - 1, 'cumulative_learning_curve'] * combined_stack_and_efficiencies_df.loc[y, 'cumulative_learning_curve']

        if not self.efficiency_degradation[0] == self.efficiency_degradation[-1]:
            raise Exception('Code does not handle degradation that varies with load factor!')

        combined_stack_and_efficiencies_df['degradation'] = self.efficiency_degradation[-1]

        combined_stack_and_efficiencies_df['final_relative_efficiency'] = 1.0

        for i in range(1, len(combined_stack_and_efficiencies_df)):
            if not combined_stack_and_efficiencies_df.loc[i, 'stack_replacement']:
                combined_stack_and_efficiencies_df.loc[i, 'final_relative_efficiency'] = combined_stack_and_efficiencies_df.loc[i - 1, 'final_relative_efficiency'] * (1 + combined_stack_and_efficiencies_df.loc[i, 'degradation'])
            else:
                combined_stack_and_efficiencies_df.loc[i, 'final_relative_efficiency'] = combined_stack_and_efficiencies_df.loc[i, 'cumulative_learning_curve']

        combined_stack_and_efficiencies_df = combined_stack_and_efficiencies_df.rename(columns={'Year': 'CalendarYear'})

        return combined_stack_and_efficiencies_df


    def _simplify_efficiencies(self, load_factor_uppers_simplified):

        self.efficiency_simplified = []

        for i in range(0,len(load_factor_uppers_simplified)):

            upper_index = [j for j, x in enumerate(self.efficiency_load_factor) if x == load_factor_uppers_simplified[i]][0]
            if i > 0:
                lower_index = [j for j, x in enumerate(self.efficiency_load_factor) if x == load_factor_uppers_simplified[i-1]][0]
            else:
                lower_index = 0

            self.efficiency_simplified.append(min(self.efficiency[lower_index:upper_index+1]))

        return self


class CombinedTank():
    def __init__(self, selected_tank, n_tanks, min_storage_kwh=2000):
        self.max_storage_kwh = selected_tank['H2 MWh Capacity'] * n_tanks * 1000
        self.leakage_percent_per_day = selected_tank['Leakage Rate (%/day)']
        self.remaining_fraction_after_half_hour = (1 - self.leakage_percent_per_day / 100) ** (1 / 48)
        self.starting_storage_kwh = self.max_storage_kwh / 2
        self.min_storage_kwh = max(min_storage_kwh, 1E-4)
        self.capex = selected_tank['CAPEX Per Unit'] * n_tanks
        self.opex_per_year = selected_tank['OPEX (£/year)'] * n_tanks
        self.floor_space = selected_tank['Floor Space (m2)'] * n_tanks



