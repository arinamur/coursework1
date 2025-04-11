import os
import tempfile

import requests
import structlog
from robogram import InlineKeyboardMarkup

from pyrob.telegram_bot.errors.errors import SkillExecutionError
from pyrob.telegram_bot.model.buttons import RobInlineButton, RunSkillInlineButton, SendMessageButton
from pyrob.telegram_bot.model.skill import (
    EmptyResult,
    FileResult,
    SkillResult,
    TextResult,
)
from pyrob.telegram_bot.skill.matcher import RobPrefixMatcher
from pyrob.telegram_bot.skill.permissions import role_oneof_permission
from pyrob.telegram_bot.skill.skill import AbstractSkill
from pyrob.telegram_bot.skill.skills.report_course_difficulty_get_skill import TaskStatus, _get_message, uuid_parameter
from pyrob.telegram_bot.telegram_context import RobTelegramContextProvider

logger = structlog.getLogger(__name__)


class ReportBannerLinksMediaGetSkill(AbstractSkill):
    name = "Получить по баннерным ссылкам"
    decription = "Отдаю готовый отчет по баннерным ссылкам"
    matchers_info = [RobPrefixMatcher("забрать отчет по баннерным ссылкам")]
    permission = role_oneof_permission("pr")
    parameters_info = [
        uuid_parameter(
            name="uuid",
            description="uuid запроса на построение отчета.",
            prompt_message="Конечно! Укажи номер обращения, и я проверю готовность отчёта.",
        )
    ]

    def run(self) -> SkillResult:
        host = os.getenv("KOLMOGOROV_HOST")
        port = os.getenv("KOLMOGOROV_PORT")
        ticket = self.parameters["uuid"].to_str()
        r = requests.get(
            f"http://{host}:{port}/reports/result_{ticket}",
            headers={"accept": "application/json;charset=utf-8", "content-type": "application/json;charset=utf-8"},
            timeout=10,
        )
        try:
            r.raise_for_status()
            logger.debug("Getting the report path was succesfull: %s", r.json())
            result = r.json()
        except requests.exceptions.HTTPError as e:
            raise SkillExecutionError(
                pretty_reason="Не получилось обработать запрос на выдачу отчёта.",
                tech_reason=f"Can't get report banner links media. Reason={e}",
            )
        status = TaskStatus(result["report_status"])
        error = result["error"]
        if error:
            return TextResult(_get_message(status, ticket=ticket))
        report_url = result["report_url"]
        if status == TaskStatus.SUCCEED:
            with tempfile.NamedTemporaryFile(delete=False) as fname:
                try:
                    response = requests.get(report_url)
                    fname.write(response.content)
                    return FileResult(
                        file_path=fname.name,
                        custom_name="report_banner.csv",
                        is_tmp_file=True,
                        caption=_get_message(status),
                    )
                except Exception as e:
                    raise SkillExecutionError(
                        pretty_reason="Не получилось обработать запрос на выдачу отчёта.",
                        tech_reason=f"Can't get report course difficulty. Reason={e}",
                    )
        elif status == TaskStatus.FAILED:
            user = self.triggered_message_info.user
            telegram_bot = RobTelegramContextProvider.get_context().telegram_bot
            buttons: list[list[RobInlineButton]] = [
                [RunSkillInlineButton(text="Попробовать ещё раз", skill_cls_name="ReportBannerLinksMediaRequestSkill")],
                [SendMessageButton(text="Не будем строить отчёт", reply_text="Понял. Не будем строить отчёт.")],
            ]
            reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons)
            telegram_bot.safe_send_message_in_telegram(
                chat_id=user.telegram_id,
                message=_get_message(status),
                disable_notification=True,
                reply_markup=reply_markup,
            )
            return EmptyResult()
        else:
            for task_status in TaskStatus:
                if task_status == status:
                    return TextResult(_get_message(task_status))
        return EmptyResult()

    @classmethod
    def abort_msg(cls) -> str:
        return "Хорошо, забудем это обсуждение."

    @classmethod
    def is_short_form(cls, message: str) -> bool:
        return True
