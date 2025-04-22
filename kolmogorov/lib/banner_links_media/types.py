from enum import Enum
from typing import Any

from fastapi import status
from fastapi.responses import JSONResponse


class KnownPartners(Enum):
    SIRIUS = "Сириус"
    SIRIUS_OLYMP = "Сириус Олимп"
    REGIONS = "Регионыльные центры"
    OLYMPRU = "Олимпиада.ру"
    FTS = "Федеральная территория Сириус"
    SIRIUS_JOURNAL = "Сириус Журнал"
    GOSPUBLICS = "Госпаблики"
    CTF = "ФКР. Фонд классных руководителей"
    TAP = "Теории и практики"
    SIRIUS_COURSES = "Сириус.Курсы"
    BIO_CPE = "БИО ЦПМ"
    LINGNEWS = "Лингвовести"
    DIPLOMARU = "Грамота.ру"
    NPR = "Национальные проекты России"
    GOSUSLUGI = "Госуслуги"
    DRD = "Департамент регионального развития"
    MSU = "МГУ"
    SIRIUS_TEACHER = "Сириус педагогам"
    EDU_ENV = "Образовательная среда"
    MIN_EDU = "Министерства просвещения"
    SCHOOL_PARTNERS = "Школы-партнеры Сириуса"


class KnownChannels(Enum):
    VK = "ВК"
    TG = "Телеграм"
    SITE = "Сайт"
    DZEN = "Дзен"
    OFFLINE = "Офлайн мероприятие/размещение"
    MAIL = "Почта"
    YOUTUBE = "Ютуб"


class EnumSkillError(Exception):
    def __init__(self, code: "ErrorCode", *args: Any) -> None:
        detail = code.template.format(*args)
        super().__init__(detail)
        self.code = code
        self.args_ = args

    def to_response(self) -> JSONResponse:
        msg = self.code.template.format(*self.args_)
        return JSONResponse(
            status_code=self.code.http_status,
            content={
                "error_code": self.code.name,
                "error": self.code.client_error,
                "error_reason": msg,
            },
        )


class ErrorCode(Enum):
    COLUMN_MISMATCH = (
        "Table columns don't match required columns",
        "Invalid columns format",
        status.HTTP_400_BAD_REQUEST,
    )
    UNKNOWN_LINK_TYPE = (
        "Unknown link type: {0}",
        "Invalid values",
        status.HTTP_404_NOT_FOUND,
    )
    UNKNOWN_CHANNEL = (
        "Unknown channel: {0}",
        "Invalid values",
        status.HTTP_404_NOT_FOUND,
    )
    UNKNOWN_PARTNER = (
        "Unknown partner: {0}",
        "Invalid values",
        status.HTTP_404_NOT_FOUND,
    )
    BANNER_GENERATION = (
        "Failed to generate banner link: {0}",
        "Failed to generate banner link",
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    CANT_PARSE_FILE = (
        "Can't parse file: {0}",
        "Can't parse file",
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    SHORT_URL_KEY_MISSING = (
        "Short URL secret key is missing",
        "Server configuration error",
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    SHORT_URL_GENERATION_FAILED = (
        "Short URL generation failed: {0}",
        "Failed to generate short URL",
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    DB_UPDATE_FAILED = (
        "Failed to update banner record in DB: {0}",
        "Failed to update banner link",
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    )

    def __init__(self, template: str, client_error: str, http_status: int):
        self.template = template
        self.client_error = client_error
        self.http_status = http_status
