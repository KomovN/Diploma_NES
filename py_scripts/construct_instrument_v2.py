import os
import fire
import pandas as pd
from typing import List
from tqdm import tqdm

SPARK_COLS = ["INN", "okved_four", "year"]

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


def prepare_weights(
        spark_path: str,
        customs_path: str,
        tariffs_df: pd.DataFrame
):
    spark_df = pd.read_parquet(spark_path)\
                .rename(columns={"Year": "year"})
    customs_df = pd.read_parquet(customs_path)

    # Filtering
    spark_df = spark_df\
            .loc[(spark_df["year"] == 2005) & (~spark_df["okved_four"].isin(['nan', 'None'])), SPARK_COLS]
    customs_df = customs_df.loc[(~customs_df["product"].isnull())]
    print(len(spark_df), len(customs_df))

    # Form the dataset
    df = pd.merge(customs_df, spark_df, on=["INN", "year"], how="inner")
    df = df.groupby(["okved_four", "product", "code"]).agg({"value": "sum"}).reset_index()\
            .assign(
                ProductCode=lambda x: x["product"].astype(int),
                current_year=2005,
                Reporter_ISO_N=lambda x: x.apply(code_to_reporter, axis=1),
            )
    print(len(df))

    df = df.merge(
        tariffs_df.loc[tariffs_df["current_year"] == 2005,["Reporter_ISO_N", "ProductCode", "SimpleAverage"]],
        on=["Reporter_ISO_N", "ProductCode"],
        how="inner"
    ).dropna(subset=["SimpleAverage"]).drop(columns="SimpleAverage")

    print(len(df))

    # Aggregate weights by okved
    agg_df = df.groupby(["okved_four"]).agg({"value": "sum"})\
                .reset_index().rename(columns={"value": "value_agg"})
    df = df.merge(agg_df, on=["okved_four"], how="inner")\
            .assign(weight=lambda x: x.value / x.value_agg)\
            .drop(columns=["value_agg"])

    # Aggregate weights by okved and country code
    agg_df = df.groupby(["okved_four", "code"]).agg({"value": "sum"})\
                .reset_index().rename(columns={"value": "value_agg"})
    df = df.merge(agg_df, on=["okved_four", "code"], how="inner")\
            .assign(weight_c=lambda x: x.value / x.value_agg)\
            .drop(columns=["value_agg"])
    
    return df


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

    df = result.merge(tariffs, on=["Reporter_ISO_N", "ProductCode", "current_year"], how="left")

    result = []
    for _, item_df in tqdm(df.groupby(["code", "product"])):
        item_df = item_df.sort_values(by=["current_year"])\
                    .assign(SimpleAverage=lambda x: x.SimpleAverage.ffill())
        result.append(item_df)

    df = pd.concat(result)

    cols = [
        "okved_four",
        "product",
        "code",
        "year",
        "value",
        "weight",
        "weight_c",
        "tariff",
        "avg_tariff"
    ]

    df = df.assign(
        year=lambda x: x["current_year"],
        tariff=lambda x: x["SimpleAverage"]
    )

    return df[cols]


def main(
        spark_path: str,
        customs_path: str,
        tariffs_path: str,
        output_path: str      
):
    tariffs = pd.read_parquet(tariffs_path)
    tariffs_agg = tariffs.groupby(["ProductCode", "current_year"])["SimpleAverage"].mean()\
                .reset_index().rename(columns={"SimpleAverage": "avg_tariff"})
    tariffs = tariffs.merge(tariffs_agg, on=["ProductCode", "current_year"], how="inner")

    weights = prepare_weights(spark_path=spark_path, customs_path=customs_path, tariffs_df=tariffs)
    df = prepare_instrument_table(weights, tariffs)

    result = []
    for _, item_df in tqdm(df.groupby(["okved_four", "product", "code"])):
        item_df = item_df.sort_values(by=["year"])\
                .assign(prev_tariff=lambda x: x.tariff.shift(1))
        result.append(item_df)

    result = pd.concat(result).assign(tariff_diff=lambda x: x.tariff - x.prev_tariff)
    result.to_parquet(output_path, index=False)


if __name__ == "__main__":
    fire.Fire(main)