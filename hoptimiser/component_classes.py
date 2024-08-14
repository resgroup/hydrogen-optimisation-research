import pandas as pd


class CombinedElectrolyser():
    def __init__(self, selected_electrolyser, n_electrolysers, stack_replacement_years, first_operational_year, component_delivery_year, n_years, electrolyser_min_capacity, reduce_efficiencies, optimise_efficiencies):

        self.rated_power = selected_electrolyser['Capacity (MW)'] * 1000 * n_electrolysers
        self.min_power = max(selected_electrolyser['Capacity (MW)'] * 1000 * electrolyser_min_capacity, 1E-4)
        self.efficiency_load_factor = selected_electrolyser['electrolyser_efficiency_load_factors'][0]
        self.efficiency = selected_electrolyser['electrolyser_efficiency'][0]
        self.full_efficiency = self.efficiency.copy()
        self.full_efficiency_load_factor = self.efficiency_load_factor.copy()

        if reduce_efficiencies:
            self._reduce_efficiencies()
            optimise_efficiencies = False

        self.capex = selected_electrolyser['CAPEX'] * n_electrolysers
        self.opex_per_year = selected_electrolyser['OPEX (£/year)'] * n_electrolysers
        self.floor_space = int(selected_electrolyser['Floor Space (m2)'] * n_electrolysers)
        self.start_stack_replacement_cost = selected_electrolyser['Start Stack Replacement Cost'] * n_electrolysers
        if optimise_efficiencies:
            self._optimise_efficiencies()
        self.stack_replacement_capex_curve = selected_electrolyser['stack_replacement_capex'][0]
        self.efficiency_degradation = selected_electrolyser['efficiency_degradation'][0]
        self.efficiency_learning_curve = selected_electrolyser['efficiency_learning'][0]
        self.stack_replacement_capex_curve_year = selected_electrolyser['stack_replacement_capex_year'][0]
        self.efficiency_learning_curve_year = selected_electrolyser['efficiency_learning_year'][0]

        if first_operational_year < min(self.stack_replacement_capex_curve_year):
            raise Exception('First operational year is not covered by stack replacement capex data, code cannot run!')

        elif first_operational_year > min(self.stack_replacement_capex_curve_year):
            years_to_remove = first_operational_year - min(self.stack_replacement_capex_curve_year)
            self.stack_replacement_capex_curve_year = self.stack_replacement_capex_curve_year[years_to_remove:]
            self.stack_replacement_capex_curve = self.stack_replacement_capex_curve[years_to_remove:]

        if first_operational_year < min(self.efficiency_learning_curve_year):
            raise Exception('First operational year is not covered by efficiency learning data, code cannot run!')

        elif first_operational_year > min(self.efficiency_learning_curve_year):
            years_to_remove = first_operational_year - min(self.efficiency_learning_curve_year)
            self.efficiency_learning_curve_year = self.efficiency_learning_curve_year[years_to_remove:]
            self.efficiency_learning_curve = self.efficiency_learning_curve[years_to_remove:]

        if component_delivery_year > first_operational_year:
            raise Exception('First operational year is earlier than capital cost data year, code cannot run!')
        elif component_delivery_year < first_operational_year: #we must add extra years at the start before operation
            years_to_add = first_operational_year - component_delivery_year
            for years in range(1, years_to_add + 1):
                year_to_add = first_operational_year - years
                self.stack_replacement_capex_curve_year = [year_to_add] + self.stack_replacement_capex_curve_year
                self.stack_replacement_capex_curve = [0.0] + self.stack_replacement_capex_curve
                self.efficiency_learning_curve_year = [year_to_add] + self.efficiency_learning_curve_year
                self.efficiency_learning_curve = [0.0] + self.efficiency_learning_curve

        self.combined_stack_and_efficiencies_df = self._combine_learning_data(first_operational_year, stack_replacement_years, n_years, years_to_add)

        self.minimum_efficiency = self.combined_stack_and_efficiencies_df['final_relative_efficiency'].min()

    def _combine_learning_data(self, first_operational_year, stack_replacement_years, n_years, years_to_add):

        efficiency_learning_df = pd.DataFrame()
        efficiency_learning_df['Year'] = self.efficiency_learning_curve_year
        efficiency_learning_df['efficiency_learning_curve'] = self.efficiency_learning_curve
        stack_replacement_df = pd.DataFrame()
        stack_replacement_df['Year'] = self.stack_replacement_capex_curve_year
        stack_replacement_df['stack_replacement_capex_curve'] = self.stack_replacement_capex_curve
        stack_replacement_df['stack_replacement'] = False
        stack_replacement_df['cumulative_capex_curve'] = 1 + stack_replacement_df['stack_replacement_capex_curve']

        stack_replacement_df = stack_replacement_df[0:n_years + years_to_add]

        for replacement_year in stack_replacement_years:
            stack_replacement_df.loc[replacement_year + years_to_add - 1, 'stack_replacement'] = True

        for y in range(1,len(stack_replacement_df)):
            stack_replacement_df.loc[y, 'cumulative_capex_curve'] = stack_replacement_df.loc[y - 1, 'cumulative_capex_curve'] * stack_replacement_df.loc[y, 'cumulative_capex_curve']

        stack_replacement_df['stack_final_capex'] = stack_replacement_df['cumulative_capex_curve'] * self.start_stack_replacement_cost

        combined_stack_and_efficiencies_df = efficiency_learning_df.merge(stack_replacement_df)

        combined_stack_and_efficiencies_df['cumulative_learning_curve'] = 1 + combined_stack_and_efficiencies_df['efficiency_learning_curve']
        for y in range(1, len(combined_stack_and_efficiencies_df)):
            combined_stack_and_efficiencies_df.loc[y, 'cumulative_learning_curve'] = combined_stack_and_efficiencies_df.loc[y - 1, 'cumulative_learning_curve'] * combined_stack_and_efficiencies_df.loc[y, 'cumulative_learning_curve']

        if not self.efficiency_degradation[0] == self.efficiency_degradation[-1]:
            raise Exception('Code does not handle degradation that varies with load factor!')

        combined_stack_and_efficiencies_df['degradation'] = self.efficiency_degradation[-1]

        combined_stack_and_efficiencies_df['final_relative_efficiency'] = 1.0

        #todo: We should account for improvement rates between the capital cost year and first year of operation. Hydra seems to assume that these are zero, hoptimiser code currently matches Hydra

        for i in range(0, len(combined_stack_and_efficiencies_df)):
            if combined_stack_and_efficiencies_df.loc[i, 'Year'] > first_operational_year:
                if not combined_stack_and_efficiencies_df.loc[i, 'stack_replacement']:
                    combined_stack_and_efficiencies_df.loc[i, 'final_relative_efficiency'] = combined_stack_and_efficiencies_df.loc[i - 1, 'final_relative_efficiency'] * (1 + combined_stack_and_efficiencies_df.loc[i, 'degradation'])
                else:
                    combined_stack_and_efficiencies_df.loc[i, 'final_relative_efficiency'] = combined_stack_and_efficiencies_df.loc[i, 'cumulative_learning_curve']

        combined_stack_and_efficiencies_df = combined_stack_and_efficiencies_df.rename(columns={'Year': 'CalendarYear'})

        return combined_stack_and_efficiencies_df

    def _optimise_efficiencies(self):

            self.efficiency[0] = 0.789
            self.efficiency[1] = 0.794
            self.efficiency[2] = 0.792

    def _reduce_efficiencies(self):
        reduced_efficiency = []
        reduced_efficiency.append(self.efficiency[0])
        reduced_efficiency.append(self.efficiency[2])
        reduced_efficiency.append(self.efficiency[4])
        reduced_efficiency.append(self.efficiency[6])
        reduced_efficiency.append(self.efficiency[8])
        reduced_efficiency.append(self.efficiency[9])

        reduced_load_factor = []
        reduced_load_factor.append(self.efficiency_load_factor[0])
        reduced_load_factor.append(self.efficiency_load_factor[2])
        reduced_load_factor.append(self.efficiency_load_factor[4])
        reduced_load_factor.append(self.efficiency_load_factor[6])
        reduced_load_factor.append(self.efficiency_load_factor[8])
        reduced_load_factor.append(self.efficiency_load_factor[9])

        self.efficiency = reduced_efficiency
        self.efficiency_load_factor = reduced_load_factor

class CombinedTank():
    def __init__(self, selected_tank, n_tanks, min_storage_kwh=2000, start_half_full=True):
        self.max_storage_kwh = selected_tank['H2 MWh Capacity'] * n_tanks * 1000
        self.leakage_percent_per_day = selected_tank['Leakage Rate (%/day)']
        self.remaining_fraction_after_half_hour = (1 - self.leakage_percent_per_day / 100) ** (1 / 48)
        if start_half_full:
            self.starting_storage_kwh = self.max_storage_kwh / 2
        else:
            self.starting_storage_kwh = 0.
        self.min_storage_kwh = max(min_storage_kwh, 1E-4)
        self.capex = selected_tank['CAPEX Per Unit'] * n_tanks
        self.opex_per_year = selected_tank['OPEX (£/year)'] * n_tanks
        self.floor_space = selected_tank['Floor Space (m2)'] * n_tanks



