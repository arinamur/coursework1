import structlog
from robogram import InlineKeyboardMarkup

from pyrob.telegram_bot.model.buttons import RobInlineButton, RunSkillInlineButton, SendMessageButton
from pyrob.telegram_bot.model.skill import EmptyResult, SkillResult
from pyrob.telegram_bot.skill.matcher import RobPrefixMatcher
from pyrob.telegram_bot.skill.permissions import role_oneof_permission
from pyrob.telegram_bot.skill.skill import AbstractSkill
from pyrob.telegram_bot.telegram_context import RobTelegramContextProvider

logger = structlog.getLogger(__name__)


class ReportBannerLinksMediaGeneralSkill(AbstractSkill):
    name = "Отчёт по баннерным ссылкам (новый)"
    description = "Делаю отчёт по баннерным ссылок"
    matchers_info = [RobPrefixMatcher("нужен отчёт по баннерным ссылкам")]
    permission = role_oneof_permission("pr")

    def run(self) -> SkillResult:
        user = self.triggered_message_info.user
        telegram_bot = RobTelegramContextProvider.get_context().telegram_bot
        buttons: list[list[RobInlineButton]] = [
            [RunSkillInlineButton(text="Построить новый отчёт", skill_cls_name="ReportBannerLinksMediaRequestSkill")],
            [RunSkillInlineButton(text="Забрать отчёт по номеру", skill_cls_name="ReportBannerLinksMediaGetSkill")],
            [SendMessageButton(text="Не будем строить отчёт", reply_text="Понял. Не будем строить отчёт.")],
        ]
        reply_markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        telegram_bot.safe_send_message_in_telegram(
            chat_id=user.telegram_id,
            message="Хочешь построить новый отчёт или забрать готовый?",
            disable_notification=True,
            reply_markup=reply_markup,
        )
        return EmptyResult()

    @classmethod
    def get_manual(cls) -> str:
        return (
            "Могу построить отчёт по баннерным ссылкам для соцсетей за любой период."
            "Отчёт оформлю в виде таблицы, пример можно посмотреть тут."
            "Чтобы запустить, напиши мне «нужен отчёт по баннерным ссылкам»."
        )

    @classmethod
    def is_short_form(cls, message: str) -> bool:
        return True
