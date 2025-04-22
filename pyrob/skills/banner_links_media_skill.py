import json
import logging
import os

import pandas as pd
import requests

from pyrob.telegram_bot.errors.errors import SkillExecutionError
from pyrob.telegram_bot.model.skill import FileResult, SkillResult
from pyrob.telegram_bot.skill.matcher import RobPrefixMatcher
from pyrob.telegram_bot.skill.parameter import file_parameter
from pyrob.telegram_bot.skill.permissions import role_oneof_permission
from pyrob.telegram_bot.skill.skill import AbstractSkill

logger = logging.getLogger(__name__)

COLUMN_RENAMES = {
    "Ссылка": "link",
    "Канал": "channel",
    "Партнёр": "partner",
    "Тип публикации": "publication_type",
    "Название публикации": "description",
    "Тип партнёра": "partner_type",
    "Техническая ссылка": "is_technical",
}

REVERSE_RENAMES = {v: k for k, v in COLUMN_RENAMES.items()}
REVERSE_RENAMES["banner_link"] = "Баннерная ссылка"


class BannerLinksMediaSkill(AbstractSkill):
    name = "Баннерные ссылки для соцсетей"
    description = "Генерирую баннерные ссылки по данным из CSV-файла."
    matchers_info = [RobPrefixMatcher("баннерные ссылки для соцсетей")]
    permission = role_oneof_permission("pr_manager", "support_manager", "smm_manager")
    parameters_info = [file_parameter(name="file", description="Файл-табличка", extension=".csv", is_optional=False)]

    def run(self) -> SkillResult:
        file = self.parameters["file"].to_str()
        df = pd.read_csv(file)
        df.rename(columns=COLUMN_RENAMES, inplace=True)
        params = {"file": json.dumps(df.to_dict(orient="records"), ensure_ascii=False)}

        host = os.getenv("KOLMOGOROV_HOST")
        port = os.getenv("KOLMOGOROV_PORT")

        r = requests.post(
            f"http://{host}:{port}/bannerLinksMedia",
            params=params,
            timeout=60,
        )

        try:
            r_json = r.json()
        except Exception:
            raise SkillExecutionError(
                pretty_reason="Не получилось сгенерировать баннерные ссылки. Попробуй позднее, пожалуйста.",
                tech_reason="Invalid JSON response from banner links service.",
            )

        error_code = r_json.get("error_code")
        client_msg = r_json.get("error")

        if r.status_code == 200:
            df = pd.DataFrame(r_json["file"])
            df = df.rename(columns=REVERSE_RENAMES)
            fname = os.path.join(os.sep, "tmp", "banner_links.csv")
            df.to_csv(fname)
            return FileResult(file_path=fname, custom_name="banner_links.csv", is_tmp_file=True, caption="Готово!")
        elif error_code == "UNKNOWN_CHANNEL":
            raise SkillExecutionError(
                pretty_reason="Не могу сгенерировать ссылки. Каналы в таблице не совпадают с теми, которые я знаю.\n"
                "Исправь их и попробуй еще раз!",
                tech_reason=f"Banner links request error. Reason={client_msg}",
            )
        elif error_code == "UNKNOWN_PARTNER":
            raise SkillExecutionError(
                pretty_reason="Не могу сгенерировать ссылки. Партнёры в таблице не совпадают с теми, которые я знаю.\n"
                "Исправь их и попробуй еще раз!",
                tech_reason=f"Banner links request error. Reason={client_msg}",
            )
        elif error_code == "UNKNOWN_LINK_TYPE":
            raise SkillExecutionError(
                pretty_reason="Не могу сгенерировать ссылки. Типы публикации в таблице не совпадают с теми, "
                "которые я знаю.\nИсправь их и попробуй еще раз!",
                tech_reason=f"Banner links request error. Reason={client_msg}",
            )
        elif error_code == "CANT_PARSE_FILE":
            raise SkillExecutionError(
                pretty_reason="Не удалось обработать файл. Убедись, что он в правильном формате и попробуй еще раз!",
                tech_reason=f"Banner links request error. Reason={client_msg}",
            )
        elif error_code == "BANNER_GENERATION":
            raise SkillExecutionError(
                pretty_reason="Не получилось сгенерировать баннерные ссылки. Попробуй позже.",
                tech_reason=f"Banner links request error. Reason={client_msg}",
            )
        else:
            raise SkillExecutionError(
                pretty_reason="Не получилось сгенерировать баннерные ссылки. Попробуй позже.",
                tech_reason=f"Unexpected error. Status={r.status_code}, Reason={client_msg}",
            )

    @classmethod
    def get_manual(cls) -> str:
        return (
            "Могу сгенерировать баннерные ссылки для соцсетей.\n\n"
            "Для генерации ссылок нужно отправить мне файл banner_links.csv."
            "Подробную информацию о заполнении файла можно прочитать [тут]("
            "https://wiki.yandex.ru/scou/departments/dev/dirs/internal/doc/rob-skills/#razmetkazakrytyxkursov).\n\n"
            "Чтобы запустить навык, напиши мне: «баннерные ссылки для соцсетей»."
        )
