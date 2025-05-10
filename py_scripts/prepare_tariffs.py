import os
import re
from tqdm import tqdm
import pandas as pd
import fire
import shutil
from zipfile import ZipFile
from typing import List


pattern_zip = re.compile(r"MFN_(H[0-6])_([A-Z]{3})_(\d{4})\.zip$")


def create_meta_data(folder: str) -> pd.DataFrame:
    """
    Создает метаданные для всех файлов с расширением .zip в папке data/raw
    :param folder: путь к папке с .zip файлами
    :return:
    """
    result = []
    not_matched = []

    for file_name in tqdm(os.listdir(folder)):
        match = pattern_zip.search(file_name)
        if match:
            try:
                with ZipFile(os.path.join(folder, file_name), 'r') as zip_ref:
                    csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]

                result.append(dict(
                    HS_standard=match.group(1),
                    country=match.group(2),
                    year=int(match.group(3)),
                    filename=match.group(0),
                    csv_files=len(csv_files),
                    csv_file=csv_files[0] if len(csv_files) > 0 else None
                ))
            except Exception as e:
                print(f"Ошибка при обработке архива {file_name}: {str(e)}")
        else:
            not_matched.append(file_name)

    return pd.DataFrame(result), not_matched


def unzip_files(folder: str, target_folder: str) -> pd.DataFrame:
    """
    Распаковывает все файлы с расширением .zip в папку data/raw
    :param folder: путь к папке с файлами
    :return:
    """
    try:
        os.mkdir(target_folder)
    except FileExistsError:
        print(f"Папка {target_folder} уже существует")
        return None
    result = []

    for file_name in tqdm(os.listdir(folder)):
        match = pattern_zip.search(file_name)
        if match:
            hs_standard = match.group(1)
            country_code = match.group(2)
            year = match.group(3)
            try:
                with ZipFile(os.path.join(folder, file_name), 'r') as zip_ref:
                    csv_files = [f for f in zip_ref.namelist() if f.lower().endswith('.csv')]
                    assert len(csv_files) == 1
                    csv_filename = csv_files.pop()
                    extracted_path = os.path.join(target_folder, f"{hs_standard}_{country_code}_{year}.csv")
                    with zip_ref.open(csv_filename) as source, open(extracted_path, 'wb') as target:
                        shutil.copyfileobj(source, target)

                result.append(dict(
                    HS_standard=hs_standard,
                    country=country_code,
                    year=int(year),
                    filename=file_name,
                    csv_file=extracted_path
                ))
            except Exception as e:
                print(f"Ошибка при обработке архива {file_name}: {str(e)}")

    return pd.DataFrame(result)


def download_tariffs(
        folder: str,
        *,
        years_of_interest: List[int] = [2005, 2006, 2007, 2008, 2009],
        cols: List[str] = ["NomenCode", "Reporter_ISO_N", "Year", "ProductCode", "SimpleAverage"]
    ) -> pd.DataFrame:
    target_folder = "{folder}_processed".format(folder=folder)
    meta_data = unzip_files(folder, target_folder)

    years = pd.DataFrame(years_of_interest, columns=["year"])
    result = []

    # Для начала подготовим список таблиц для выгрузки
    for _, item_df in meta_data.groupby("country"):
        item_df = item_df.assign(tariff_year=lambda x: x.year)\
                    .merge(years, on="year", how="outer")\
                    .sort_values(by="year")\
                    .ffill()
        result.append(item_df)

    result = pd.concat(result).dropna()
    meta_data = result.loc[result["year"].between(years_of_interest[0], years_of_interest[-1])]

    result = []

    # Займемся выгрузкой
    for _, item in tqdm(meta_data.iterrows()):
        file_name = item.csv_file
        current_year = item.year
        country = item.country

        res = pd.read_csv(file_name).loc[:,cols]\
                .assign(
                    country=country,
                    current_year=current_year
                )
        result.append(res)

    return pd.concat(result), meta_data


def main(
        folder: str,
        target_path: str,
        years_of_interest: List[int] = [2005, 2006, 2007, 2008, 2009],
        cols: List[str] = ["NomenCode", "Reporter_ISO_N", "Year", "ProductCode", "SimpleAverage"]
    ):
    df, _ = download_tariffs(folder, years_of_interest=years_of_interest, cols=cols)
    df = df.drop_duplicates()

    # Уберем страны без вариации тарифов!
    # bad_countries = df.loc[(df.current_year == 2009) & (df.Year <= 2005), "country"].unique()
    # df = df.loc[~df["country"].isin(bad_countries)]

    df.to_parquet(target_path, index=False)


if __name__ == "__main__":
    fire.Fire(main)
