import pandas as pd

from lib.skill_executions.banner_link.link import BannerLinkType
from lib.skill_executions.banner_links_media.types import EnumSkillError, ErrorCode, KnownChannels, KnownPartners

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

REQUIRED_COLUMNS = ["link", "channel", "partner", "publication_type", "partner_type"]


def get_banner_type(type_: str) -> BannerLinkType:
    if type_ not in LINK_TYPE_TRANSLATION:
        raise EnumSkillError(ErrorCode.UNKNOWN_LINK_TYPE, type_)
    return LINK_TYPE_TRANSLATION[type_]


def validate_columns(data: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in data.columns]
    if missing:
        raise EnumSkillError(ErrorCode.COLUMN_MISMATCH)


def validate_values(data: pd.DataFrame) -> None:
    for _, row in data.iterrows():
        channel = str(row["channel"]).strip()
        try:
            KnownChannels(channel)
        except ValueError as e:
            raise EnumSkillError(ErrorCode.UNKNOWN_CHANNEL, channel) from e
        partner = str(row["partner"]).strip()
        try:
            KnownPartners(partner)
        except ValueError as e:
            if row["partner_type"].strip() != "+":
                raise EnumSkillError(ErrorCode.UNKNOWN_PARTNER, partner) from e
        publication_type = str(row["publication_type"]).strip()
        get_banner_type(publication_type)


def validation(df: pd.DataFrame) -> EnumSkillError | None:
    try:
        validate_columns(df)
        validate_values(df)
    except EnumSkillError as e:
        return e
    return None
