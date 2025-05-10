import os
import fire
import pandas as pd

DATA_DIR = "/Users/mac/Desktop/Study/Diploma/data"
COUNTRIES_DIR = os.path.join(DATA_DIR, "countries")
WITS_PATH = os.path.join(COUNTRIES_DIR, "WITS_codes.xlsx")
RUS_PATH = os.path.join(COUNTRIES_DIR, "rus_countries.csv")
YUGOSLAVIA = [499, 688]

"""
Описание полей входной таблицы:
- g012: таможенный режим
- g021: ИНН фирмы
- g023: адрес фирмы, указанный в декларации
- g33: код товара ТНВЭД
- g34: код страны происхождения
- g46: статистическая стоимость
"""


def process_code(item, *, name_to_code, all_codes):
    if str(item).isalpha():
        if item == "АВ":
             item = "AB"
        code = name_to_code.get(item, 0)
    else:
        item = int(item)
        code = item if item in all_codes else 0
        
    return code if not code in YUGOSLAVIA else 891


def process_product_code(item):
    try:
        item = int(item)
        return item // 10000
    except ValueError:
        return None


def return_cleaned_data(data_path):
    data = pd.read_csv(data_path, low_memory=False)\
            .drop(columns=["Unnamed: 0", "nd", "g012", "g15a"]) # Пока не дропаем g33
    print(len(data))

    # Избавляемся от пустых значений
    data = data.dropna(subset=["g021", "g17a", "g46"])
    data = data[data.g021.str.isnumeric()].assign(g021=lambda x: x.g021.astype(int))
    print(len(data))

    # Обрабатываем коды стран
    codes = pd.read_csv(RUS_PATH)
    all_codes = set(codes["code"].unique())
    name_to_code = {key: val for key, val in zip(codes["RUS_ISO2"], codes["code"])}
    func = lambda x: process_code(x, name_to_code=name_to_code, all_codes=all_codes)
    data = data.assign(
        code=lambda x: x.g17a.map(func),
        product=lambda x: x.g33.map(process_product_code, na_action="ignore"),
    )
    data = data.loc[~data.code.isin([0, 643])] # 643 - Россия, 0 - неизвестно
    print("Final size is {}".format(len(data)))

    to_rename = {"g021": "INN", "g46": "value"}
    return data.drop(columns=["g023", "g17a", "g072", "gd1", "g34", "g33"])\
                .rename(columns=to_rename)


def main(data_path: str, output_path: str):
    years = [2005, 2006, 2007, 2008, 2009]

    for year in years:
        print(20 * '-')
        print("Processing {year}".format(year=year))
        data = return_cleaned_data(os.path.join(data_path, "gtd{year}.csv".format(year=year)))\
                    .assign(year=year)
        data = data.to_parquet(os.path.join(output_path, "gtd{year}.parquet".format(year=year)))


if __name__ == "__main__":
    fire.Fire(main)