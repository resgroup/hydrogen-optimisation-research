from typing import Dict, List, Union
from dataclasses import dataclass


@dataclass(frozen=True)
class Stack:
    oem: str
    kind: str
    rated_power: float
    count: int
    min_power_ratio: float
    max_power_ratio: float
    efficiency_curve_base_year: int
    efficiency_table: Dict[str, List[float]]
    efficiency_learning_curve: Dict[str, Union[List[int], List[float]]]
    degradation_rate_per_year: float
    output_pressure_bar: float
    capex: float
    opex_per_year: float
    initial_replacement_cost: float
    replacement_learning_curve: float
    floor_area: float

    def __post_init__(self):
        object.__setattr__(self, 'min_rated_power', self.rated_power * self.min_power_ratio)
        object.__setattr__(self, 'max_rated_power', self.rated_power * self.max_power_ratio)
