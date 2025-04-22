import json
import os
from typing import Any

import pandas as pd
import requests
from requests.exceptions import HTTPError

from lib.skill_executions.banner_link.link import BannerLink
from lib.skill_executions.banner_links_media.types import EnumSkillError, ErrorCode, KnownChannels
from lib.skill_executions.banner_links_media.validation import get_banner_type
from lib.time import now_time_msk, output_time


def generate_link(line: pd.Series, is_test: bool = False) -> tuple[Any, Any]:  # type: ignore[type-arg]
    description = (
        f"Канал: {line['channel']}.\n"
        f"Партнёр: {line['partner']}.\n"
        f"Название публикации: {line['description']}.\n"
        f"Дата генерации: {output_time(now_time_msk())[:10]}."
    )

    try:
        banner_link, banner_id = BannerLink.create_banner_link(
            link=str(line["link"]),
            link_type=get_banner_type(str(line["publication_type"])),
            description=description,
            return_id=True,
        )
    except Exception as e:
        return None, EnumSkillError(ErrorCode.BANNER_GENERATION, str(e))

    # Короткая ссылка
    if KnownChannels(str(line["channel"]).strip()) in [KnownChannels.VK, KnownChannels.TG] and not is_test:
        key = os.getenv("SHORT_URL_SECRET_KEY")
        if key is None:
            return None, EnumSkillError(ErrorCode.SHORT_URL_KEY_MISSING)

        try:
            r = requests.post(
                "https://lab.sirius.online/lab-noo/developer/shorten-link",
                data=json.dumps({"longLink": banner_link}),
                headers={
                    "accept": "application/json;charset=utf-8",
                    "Authorization": key,
                    "content-type": "application/json;charset=utf-8",
                },
                timeout=10,
            )
            r.raise_for_status()
            if "newShortLink" in r.json().get("success", {}):
                banner_link = r.json()["success"]["newShortLink"]
        except HTTPError as e:
            return None, EnumSkillError(ErrorCode.SHORT_URL_GENERATION_FAILED, str(e))

    return banner_id, banner_link
