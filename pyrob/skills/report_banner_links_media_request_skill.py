import json
import os
import tempfile

import requests
import structlog

from pyrob.telegram_bot.errors.errors import SkillExecutionError
from pyrob.telegram_bot.model.skill import (
    FileResult,
    SkillResult,
    TextResult,
)
from pyrob.telegram_bot.skill.matcher import RobPrefixMatcher
from pyrob.telegram_bot.skill.parameter import date_parameter
from pyrob.telegram_bot.skill.permissions import role_oneof_permission
from pyrob.telegram_bot.skill.skill import AbstractSkill

logger = structlog.getLogger(__name__)


class ReportBannerLinksMediaRequestSkill(AbstractSkill):
    name = "Запустить построение отчёта по банненрым ссылкам"
    description = "Запускаю построение отчёта по банненрым ссылкам"
    matchers_info = [RobPrefixMatcher("запустить построение отчёта по банненрым ссылкам")]
    permission = role_oneof_permission("pr")
    parameters_info = [
        date_parameter(
            name="from_date",
            description="дата начала для построения отчета в формате «25.05.2025»",
            date_format="%d.%m.%Y",
        ),
        date_parameter(
            name="to_date",
            description="дата конца для построения отчета в формате «25.05.2025»",
            date_format="%d.%m.%Y",
        ),
    ]

    def run(self) -> SkillResult:
        host = os.getenv("KOLMOGOROV_HOST")
        port = os.getenv("KOLMOGOROV_PORT")
        data = json.dumps(
            {
                "report_name": "ReportBannerLinksMedia",
                "parameters": {
                    "from_date": str(self.parameters["from_date"].value),
                    "to_date": str(self.parameters["to_date"].value),
                },
            }
        )
        try:
            r = requests.post(
                f"http://{host}:{port}/reports/start",
                headers={"accept": "application/json", "content-type": "application/json"},
                data=data,
                timeout=10,
            )

            r.raise_for_status()
            result = json.loads(r.content)

        except Exception as e:
            raise SkillExecutionError(
                pretty_reason="Не получилось запустить построение отчёта. Попробуй позже.",
                tech_reason=f"Can't get data from kolmogorov-service. Reason={e}",
            )
        ticket = result["ticket"]
        error = result["error"]
        report_url = result["report_url"]
        if error is not None:
            return TextResult("Не получилось построить отчет. Попробуй позже.")
        if report_url is not None:
            try:
                with tempfile.NamedTemporaryFile(delete=False) as fname:
                    response = requests.get(report_url)
                    fname.write(response.content)
                    return FileResult(
                        file_path=fname.name,
                        custom_name="report_banner.csv",
                        is_tmp_file=True,
                        caption="В базе уже есть такой отчёт.",
                    )
            except Exception as e:
                raise SkillExecutionError(
                    pretty_reason="Не получилось запустить построение отчёта.",
                    tech_reason=f"Report file exists, but can't download it. Reason={e}",
                )
        return TextResult(
            (
                f"Запрос на построение отчёта по баннерным ссылкам принят!"
                "\n \n"
                "Номер обращения:\n"
                f"`{ticket}`"
                "\n \n"
                "Номер обращения нужно будет указать, чтобы узнать статус построения отчёта "
                "или получить готовый отчёт."
            )
        )

    @classmethod
    def is_short_form(cls, message: str) -> bool:
        return True
