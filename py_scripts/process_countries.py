import os
import fire
import pandas as pd

def prepare_rus_table(target_dir: str):
    """
    This function prepares table with countries codes and their names
    from Russian classification of countries of the world.
    """
    url = "https://ru.wikipedia.org/wiki/%D0%9E%D0%B1%D1%89%D0%B5%D1%80%D0%BE%D1%81%D1%81%D0%B8%D0%B9%D1%81%D0%BA%D0%B8%D0%B9_%D0%BA%D0%BB%D0%B0%D1%81%D1%81%D0%B8%D1%84%D0%B8%D0%BA%D0%B0%D1%82%D0%BE%D1%80_%D1%81%D1%82%D1%80%D0%B0%D0%BD_%D0%BC%D0%B8%D1%80%D0%B0"
    df = pd.read_html(url)[0]

    df.columns = ["country_name", "full_name", "RUS_ISO2", "RUS_ISO3", "code"]

    # Заменим Судан!
    df.loc[df["RUS_ISO2"] == "SD", "code"] = 736

    # Добавим две страны, которых не было в списке
    df = pd.concat([
        df,
        pd.DataFrame(zip(
            ["АНТИЛЬСКИЕ О-ВА", "СЕРБИЯ И ЧЕРНОГОРИЯ"],
            ["АНТИЛЬСКИЕ О-ВА", "СЕРБИЯ И ЧЕРНОГОРИЯ"],
            ["AN", "CS"],
            ["ANT", "SER"],
            [530, 891]
        ),
        columns=df.columns)
    ]).reset_index().drop(columns="index")

    df.to_csv(os.path.join(target_dir, "rus_countries.csv"), index=False)


def prepare_table(WITS_path: str, rus_path: str):
    WITS = pd.read_excel(WITS_path)
    RUS_ISO = pd.read_csv(rus_path)

    df = pd.merge(RUS_ISO, WITS, on="code", how="left")

    return df.drop(columns=["full_name"])


if __name__ == "__main__":
    # Запускаем командой python prepare_countries.py table_dir
    fire.Fire(prepare_rus_table)