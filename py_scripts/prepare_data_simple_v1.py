import os
import fire
import numpy as np
import pandas as pd
from tqdm import tqdm

FINAL_COLS = [
    "year",
    "okved_four",
    "log_assets",
    "tangibility",
    "profitability",
    "instrument",
    "instrument_c",
    "leverage",
    "short_leverage",
    "long_leverage",
    "empl",
    "num_countries",
    "num_countries_prev_log",
    "num_deliveries_prev_log",
    "value_prev_log",
    "countries_diff",
    "countries_diff_prev",
    "expansion",
    "exporting"
]


def return_unique(x):
    return len(x.unique())


def prepare_gtd_df(gtd_path: str):
    TO_RENAME = {
        "code": "num_countries",
        "product": "num_deliveries"
    }
    gtd_tables = os.listdir(gtd_path)
    gtd_df = []
    for gtd_file in tqdm(gtd_tables):
        df = pd.read_parquet(os.path.join(gtd_path, gtd_file))
        df.columns = [item.lower() for item in df.columns]
        try:
            df = df.assign(value=lambda x: x.value.str.replace(',', '.').astype(float))
        except AttributeError:
            df = df.assign(value=lambda x: x.value.astype(float))
        df = df.loc[(df["inn"] > 100) & (~df["product"].isnull())]\
                .groupby(["inn", "year"]).agg({"code": return_unique, "product": "count", "value": "sum"})\
                .reset_index().rename(columns=TO_RENAME)
        gtd_df.append(df)
    gtd_df = pd.concat(gtd_df)
    print("Len of GTD table: {}".format(len(gtd_df)))
    return gtd_df


def prepare_iv_df(iv_path: str):
    iv_df = pd.read_parquet(iv_path)\
            .assign(instrument=lambda x: x["weight"] * (x["tariff"]) / 100)\
            .assign(instrument_c=lambda x: x["weight_c"] * (x["tariff"]) / 100)
    
    iv_df = iv_df.groupby(["okved_four", "year"])[["instrument", "instrument_c"]].sum().reset_index()
    print("Len of IV table: {}".format(len(iv_df)))
    return iv_df


def join_all_tables(
        spark_df: pd.DataFrame,
        ruslana_df: pd.DataFrame,
        gtd_df: pd.DataFrame,
        iv_df: pd.DataFrame
):
    df = spark_df.merge(ruslana_df, on=["inn", "year"], how="inner")\
                .merge(iv_df, on=["okved_four", "year"], how="inner")\
                .merge(gtd_df, on=["inn", "year"], how="outer")\
                .drop_duplicates(["inn", "year"])
    
    years = pd.DataFrame(np.arange(2004, 2010), columns=["year"])
    export_data = []
    for inn, item_df in tqdm(df.loc[~df.num_countries.isnull()].groupby("inn")):
        item_df = item_df.merge(years, on="year", how="right")\
                    .assign(
                        inn=inn,
                        num_countries=lambda x: x.num_countries.fillna(0.0), 
                        num_countries_prev=lambda x: x.num_countries.shift(1),
                        countries_diff=lambda x: x.num_countries - x.num_countries_prev,
                        countries_diff_prev=lambda x: x.countries_diff.shift(1),
                        num_deliveries=lambda x: x.num_deliveries.fillna(0.0),
                        num_deliveries_prev=lambda x: x.num_deliveries.shift(1),
                        value=lambda x: x.value.fillna(0.0),
                        value_prev=lambda x: x.value.shift(1),
                    )

        export_data.append(item_df)

    export_data = pd.concat(export_data)\
                    .loc[:,["inn", "year", "num_countries_prev", "countries_diff", "countries_diff_prev", "num_deliveries_prev", "value_prev"]]
    
    df = df.merge(export_data, on=["inn", "year"], how="left")\
            .assign(
                num_countries=lambda x: x.num_countries.fillna(0.0),
                num_countries_prev_log=lambda x: np.log(1 + x.num_countries_prev.fillna(0.0)),
                num_deliveries_prev_log=lambda x: np.log(1 + x.num_deliveries_prev.fillna(0.0)),
                value_prev_log=lambda x: np.log(1 + x.value_prev.fillna(0.0)),
                countries_diff=lambda x: x.countries_diff.fillna(0.0),
                countries_diff_prev=lambda x: x.countries_diff_prev.fillna(0.0),
                expansion=lambda x: 1 * (x.countries_diff > 0.0),
                exporting=lambda x: 1 * (x.num_countries > 0.0)
            )
    print("Len of merged table: {}".format(len(df)))
    return df
        

def filter_data(df: pd.DataFrame):
    filter_cond = (df.assets > 0.)
    data = df.loc[filter_cond]\
            .sort_values(by=["inn", "year"])\
            .assign(
                short_leverage=lambda x: x.short_debt / x.assets, 
                long_leverage=lambda x: x.long_debt / x.assets, 
                leverage=lambda x: x.debt / x.assets, 
                log_assets=lambda x: np.log(x.assets),
                tangibility=lambda x: x.tang_assets / x.assets, 
                profitability=lambda x: x.revenue / x.assets
            )
    print("Assets more then 0: {}".format(len(data)))

    left, right = data.assets.quantile([0.025, 0.975]).tolist()
    filter_cond = (
        (data["short_leverage"] >= 0.) &
        (data["long_leverage"] >= 0.) &
        (data["leverage"] <= 1.) &
        (data.revenue > 0.0) &
        (data.assets > left) &
        (data.assets < right) &
        (data.empl >= 5.0)
    )

    data = data.loc[filter_cond]
    print("Employees no less than 5: {}".format(len(data)))

    data = data.loc[data.year > 2005]
    print("Dataset length: {}".format(len(data)))

    return data


def write_to_csv(df: pd.DataFrame, output_path: str):
    data = df.dropna(subset=["inn"] + FINAL_COLS).drop_duplicates(subset=["inn", "year"])
    inns = data["inn"].unique()

    inns = pd.DataFrame(inns, columns=["inn"])\
        .reset_index().rename(columns={"index": "firm_id"})

    data = data.merge(inns, on=["inn"], how="inner")\
                .assign(alternative_iv = lambda x: x["instrument"] * (1 + x["num_countries_prev_log"]))
    print("Final dataset length: {}".format(len(data)))
    data.loc[:,["firm_id"] + FINAL_COLS + ["alternative_iv"]].to_csv(output_path, index=False)


def main(
        spark_path: str,
        ruslana_path: str,
        gtd_path: str,
        iv_path: str,
        output_path: str
):
    spark_df = pd.read_parquet(spark_path)
    spark_df.columns = [item.lower() for item in spark_df.columns]
    print("Len of Spark table: {}".format(len(spark_df)))

    ruslana_df = pd.read_parquet(ruslana_path)\
                    .drop_duplicates()
    ruslana_df.columns = [item.lower() for item in ruslana_df.columns]
    ruslana_agg = ruslana_df.drop_duplicates().groupby(["inn", "year"]).count().reset_index().sort_values(by="empl")
    ruslana_agg = ruslana_agg.loc[ruslana_agg.empl == 1].drop("empl", axis=1)
    ruslana_df = ruslana_agg.merge(ruslana_df.drop_duplicates(), on=["inn", "year"], how="left")
    print("Len of Ruslana table: {}".format(len(ruslana_df)))

    gtd_df = prepare_gtd_df(gtd_path)

    iv_df = prepare_iv_df(iv_path)

    df = join_all_tables(spark_df, ruslana_df, gtd_df, iv_df)
    data = filter_data(df)
    write_to_csv(data, output_path)
    print("Data saved to {}".format(output_path))


if __name__ == "__main__":
    fire.Fire(main)
