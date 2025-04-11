import re

import pandas as pd

KNOWN_PARTNERS = [
    "Сириус",
    "Сириус Олимп",
    "Регионыльные центры",
    "Олимпиада.ру",
    "Федеральная территория Сириус",
    "Сириус Журнал",
    "Госпаблики",
    "ФКР. Фонд классных руководителей",
    "Теории и практики",
    "Сириус.Курсы",
    "БИО ЦПМ",
    "Лингвовести",
    "Грамота.ру",
    "Национальные проекты России",
    "Госуслуги",
    "Департамент регионального развития",
    "МГУ",
    "Сириус педагогам",
    "Образовательная среда",
    "Министерства просвещения",
    "Школы-партнеры Сириуса",
]


def parse(title: str) -> dict[str, str]:
    pattern = (
        r"Соцсеть:\s*(.*?)\.\s*"
        r"(?:Паблик/профиль|Профиль):\s*(.*?)\.\s*"
        r"Тип публикации:\s*(.*?)\.\s*"
        r"Дата публикации:\s*(.*?)\.\s*"
        r"Название публикации:\s*(.*?)\."
    )

    match = re.search(pattern, title)
    if not match:
        return {"title": title}

    channel, partner, type_publication, date_publication, name_publication = match.groups()
    partner_type = "внутренний" if partner.strip() in KNOWN_PARTNERS else "внешний"
    return {
        "channel": channel.strip(),
        "partner": partner.strip(),
        "partner_type": partner_type,
        "publication_type": type_publication.strip(),
        "title": name_publication.strip(),
    }


def table_parse(df: pd.DataFrame) -> pd.DataFrame:
    parsed_columns = df["title"].apply(lambda x: pd.Series(parse(x)))
    df.update(parsed_columns)
    return df
