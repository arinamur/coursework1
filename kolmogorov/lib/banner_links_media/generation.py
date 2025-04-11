import json
import os
from typing import Any

import pandas as pd
import requests
from fastapi import status
from requests.exceptions import HTTPError

from lib.skill_executions.banner_link.link import BannerLink
from lib.skill_executions.banner_links_media.validation import get_banner_type
from lib.time import now_time_msk, output_time


def generate_link(line: pd.Series, is_test: bool = False) -> tuple[Any, Any]:  # type: ignore[type-arg]
    description = f"Канал: {line['channel']}.\nПартнёр: {line['partner']}.\nНазвание публикации: " \
                  f"{line['description']}.\nДата генерации: {output_time(now_time_msk())[:10]}."
    try:
        banner_link, banner_id = BannerLink.create_banner_link(
            link=str(line["link"]),
            link_type=get_banner_type(str(line["publication_type"])),
            description=description,
            return_id=True,
        )
    except Exception as e:
        return status.HTTP_500_INTERNAL_SERVER_ERROR, e

    # Короткая ссылка
    if str(line["channel"]).strip() in ["ВК", "Телеграм"] and not is_test:
        key = os.getenv("SHORT_URL_SECRET_KEY")
        if key is None:
            return status.HTTP_500_INTERNAL_SERVER_ERROR, "no secret key"
        data = {
            "longLink": banner_link,
        }

        r = requests.post(
            "https://lab.sirius.online/lab-noo/developer/shorten-link",
            data=json.dumps(data),
            headers={
                "accept": "application/json;charset=utf-8",
                "Authorization": key,
                "content-type": "application/json;charset=utf-8",
            },
            timeout=10,
        )

        try:
            r.raise_for_status()
        except HTTPError as e:
            return status.HTTP_500_INTERNAL_SERVER_ERROR, e
        if "newShortLink" in r.json().get("success"):
            banner_link = r.json()["success"]["newShortLink"]
    return banner_id, banner_link
