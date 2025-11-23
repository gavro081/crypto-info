from abc import abstractmethod, ABC
import pandas as pd

class Filter(ABC):
    @abstractmethod
    def apply(self, df: pd.DataFrame) -> pd.DataFrame:
        pass