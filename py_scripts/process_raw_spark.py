import os
import re
import fire
import pandas as pd
from typing import Optional


def extract_okved(item: str) -> str:
    item = str(item).strip()
    match = re.search(r'\b\d{2}\.\d{2}\b', item)
    return match.group(0) if match else None


def process_raw_data(df: pd.DataFrame, *, source: str="CUR") -> pd.DataFrame:
    COLUMNS = [
        "INN",
        "OKVED",
        "Year",
        "Form_1_Field_290",
        "Form_1_Field_300",
        "Form_1_Field_690",
        "Form_2_Field_010",
        "Form_2_Field_100",
        "Form_2_Field_190",
        "Form_1_Field_590"
    ]

    TO_RENAME = dict(
        Form_1_Field_290="tang_assets", 
        Form_1_Field_300="assets",
        Form_1_Field_590="long_debt",
        Form_1_Field_690="short_debt",
        Form_2_Field_010="revenue",
        Form_2_Field_100="opex",
        Form_2_Field_190="profit"
    )

    DROPNA_SUBSET = ["INN", "assets"] # short_debt, long_debt

    return df.loc[(df.Source == source) & (df.Year > 2004), COLUMNS]\
            .rename(columns=TO_RENAME)\
            .dropna(subset=DROPNA_SUBSET)\
            .assign(
                INN=lambda x: x.INN.astype(int),
                short_debt=lambda x: x.short_debt.fillna(0),
                long_debt=lambda x: x.long_debt.fillna(0),
                debt=lambda x: x.short_debt + x.long_debt,
                okved_four=lambda x: x.OKVED.map(extract_okved, na_action="ignore").astype(str),
                OKVED=lambda x: x.OKVED.astype(str)
            )


def main(data_dir: str, output_path: str=None, source: str="CUR") -> Optional[pd.DataFrame]:
    files = os.listdir(data_dir)

    result = []
    for file_name in files:
        if file_name.endswith(".csv"):
            print("Processing {file}".format(file=file_name))
            try:
                df = pd.read_csv(os.path.join(data_dir, file_name), sep=';', low_memory=False)
                df = process_raw_data(df, source=source)
                print(df.shape)
                result.append(df)
            except KeyError as e:
                print("Failed to process {file}: {error}".format(file=file_name, error=e))

    print("All files processed!")

    result = pd.concat(result)
    print(len(result))
    if output_path is not None:
        result.to_parquet(output_path, index=False)
    else:
        return result
    

if __name__ == "__main__":
    fire.Fire(main)