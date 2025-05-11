import os
import fire
import pandas as pd
from typing import List

# Пара код страны, год вхождения в EU
EU = [
    (40, 1995), (56, 1957), (100, 2007), (348, 2004), (276, 1957), (300, 1981), (208, 1973),
    (372, 1973), (724, 1986), (380, 1957), (196, 2004), (428, 2004), (440, 2004), (442, 1957),
    (470, 2004), (528, 1957), (616, 2004), (620, 1986), (642, 2007), (703, 2004), (705, 2004),
    (246, 1995), (250, 1957), (203, 2004), (752, 1995), (233, 2004), (826, 1957)
]

EU_DICT = {key: val for key, val in EU}


def code_to_reporter(item):
    code = item.code
    current_year = item.current_year

    if code in EU_DICT.keys():
        entry_year = EU_DICT[code]
        if current_year >= entry_year:
            return 918
    return code


def prepare_instrument_table(
        weights: pd.DataFrame,
        tariffs: pd.DataFrame,
        years_of_interest: List[int] = [2005, 2006, 2007, 2008, 2009]
) -> pd.DataFrame:
    result = []
    for year in years_of_interest:
        df = weights.assign(
            current_year=year,
            Reporter_ISO_N=lambda x: x.apply(code_to_reporter, axis=1)
        )
        result.append(df)

    result = pd.concat(result)

    df = result.merge(tariffs, on=["Reporter_ISO_N", "ProductCode", "current_year"], how="inner")

    cols = [
        "okved_four",
        "product",
        "code",
        "year",
        "value",
        "weight",
        "weight_c",
        "tariff"
    ]

    df = df.assign(
        year=lambda x: x["current_year"],
        tariff=lambda x: x["SimpleAverage"]
    )

    return df[cols]


def main(weights_path: str, tariffs_path: str, output_path: str):
    weights = pd.read_parquet(weights_path)\
            .assign(ProductCode=lambda x: x["product"].astype(int))
    tariffs = pd.read_parquet(tariffs_path)

    result = prepare_instrument_table(weights=weights, tariffs=tariffs)
    result.to_parquet(output_path, index=False)


if __name__ == "__main__":
    fire.Fire(main)