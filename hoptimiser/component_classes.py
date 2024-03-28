class CombinedElectrolyser():
    def __init__(self, selected_electrolyser, n_electrolysers, electrolyser_min_capacity = 0.1):

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



