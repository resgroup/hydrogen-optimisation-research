from hoptimiser.variable_price_orchestrator import Analysis
from hoptimiser.component_inputs_reader import read_component_data, populate_combinations
import os
from hoptimiser.config import PROJECT_ROOT_DIR


if __name__ == "__main__":

    input_file_name_components = 'component_inputs.xlsx'

    tank_df, electrolyser_df, data_years = read_component_data(os.path.join(PROJECT_ROOT_DIR, 'inputs', input_file_name_components))
    combinations = populate_combinations(tank_df, electrolyser_df, os.path.join(PROJECT_ROOT_DIR, 'inputs', input_file_name_components))

    for combination in combinations:
        analysis = Analysis(
            input_combination=str(combination), run_in_azure=False
        )

        analysis.run()

