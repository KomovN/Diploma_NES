import fire
import pandas as pd

SPARK_COLS = ["INN", "okved_four", "year"]


def construct_weights(spark_path: str, customs_path: str):
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
    df = df.groupby(["okved_four", "product", "code"]).agg({"value": "sum"}).reset_index()

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


def main(spark_path: str, customs_path: str, output_path: str):
    df = construct_weights(spark_path, customs_path)
    df.to_parquet(output_path, index=False)


if __name__ == "__main__":
    fire.Fire(main)



