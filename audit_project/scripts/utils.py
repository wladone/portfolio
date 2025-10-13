import pandas as pd
import random
from typing import List

def load_data(file_paths: List[str], keys: List[str]) -> dict:
    data = {}
    for path, key in zip(file_paths, keys):
        df = pd.read_csv(path)
        data[key] = df
    return data

def sample_dataframe(df: pd.DataFrame, fraction: float = 0.1, seed: int = 42) -> pd.DataFrame:
    return df.sample(frac=fraction, random_state=seed)
