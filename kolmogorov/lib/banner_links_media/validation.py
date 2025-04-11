from typing import Any

import pandas as pd
from fastapi import status

from lib.skill_executions.banner_link.link import BannerLinkType

LINK_TYPE_TRANSLATION = {
    "пост": BannerLinkType.POST,
    "новость": BannerLinkType.NEWS,
    "баннер": BannerLinkType.BANNER,
    "презентация": BannerLinkType.PRESENTATION,
    "посм": BannerLinkType.POSM,
    "ссылка": BannerLinkType.LINK,
    "карточка": BannerLinkType.CARD,
    "кнопка": BannerLinkType.BUTTON,
    "кьюар": BannerLinkType.QR,
}

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

KNOWN_CHANNELS = ["ВК", "Телеграм", "Дзен", "Сайт", "Офлайн мероприятие/размещение", "Почта", "Ютуб"]


# Временный костыль
def get_banner_type(type_: str) -> BannerLinkType:
    if type_ not in LINK_TYPE_TRANSLATION:
        raise ValueError(type_)
    return LINK_TYPE_TRANSLATION[type_]


def validate_columns(data: pd.DataFrame) -> None:
    required_columns = ["link", "channel", "partner", "publication_type", "partner_type"]
    missing = [col for col in required_columns if col not in data.columns]
    if missing:
        raise ValueError


def validate_values(data: pd.DataFrame) -> None:
    for _, row in data.iterrows():
        channel = str(row["channel"])
        if channel.strip() not in KNOWN_CHANNELS:
            raise ValueError(channel)
        partner_name = str(row["partner"])
        if partner_name.strip() not in KNOWN_PARTNERS:
            outer = row["partner_type"].strip()
            if outer != "+":
                raise ValueError(partner_name)
        publication_type = str(row["publication_type"]).strip()
        get_banner_type(publication_type)


def validation(df: pd.DataFrame) -> tuple[Any, ValueError | None]:
    try:
        validate_columns(df)
    except ValueError as e:
        return status.HTTP_400_BAD_REQUEST, e
    try:
        validate_values(df)
    except ValueError as e:
        return status.HTTP_400_BAD_REQUEST, e
    return None, None
