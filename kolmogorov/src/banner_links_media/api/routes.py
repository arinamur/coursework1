import json
import logging

import pandas as pd
import psycopg2
from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from lib.skill_executions.banner_links_media.generation import generate_link
from lib.skill_executions.banner_links_media.repo import AnalyticsBannerLinks
from lib.skill_executions.banner_links_media.validation import validation
from src.skill_executions.repo import SkillExecutionsRepo

logger = logging.getLogger(__name__)

banner_link_media_router = APIRouter()


@banner_link_media_router.post("/bannerLinksMedia")
def banner_links_media(file: str, request: Request) -> JSONResponse:
    df = pd.DataFrame(json.loads(file)).fillna("")
    kolmogorov_repo = SkillExecutionsRepo(request.app.state.db_kolmogorov)
    data = {"records": df.to_dict(orient="records")}
    register_id = kolmogorov_repo.register_running_request("BannerLinksMediaReport", data)
    banner_repo = AnalyticsBannerLinks(request.app.state.db_analytics)

    df = df.dropna(how="all")

    # Валидация таблички
    stat, error = validation(df)
    if stat == status.HTTP_400_BAD_REQUEST:
        kolmogorov_repo.register_request_err(register_id)
        logger.error("Invalid columns format")
        return JSONResponse(
            content={
                "error": "Invalid columns format",
                "error_reason": str(error),
            },
            status_code=status.HTTP_400_BAD_REQUEST,
        )
    elif stat == status.HTTP_404_NOT_FOUND:
        kolmogorov_repo.register_request_err(register_id)
        logger.error("Invalid values")
        return JSONResponse(
            content={
                "error": "Invalid values",
                "error_reason": str(error),
            },
            status_code=status.HTTP_404_NOT_FOUND,
        )

    # Генерация баннерных ссылок
    is_test = False
    banner_links = []
    for _, line in df.iterrows():
        val, error = generate_link(line, is_test)
        if val == status.HTTP_500_INTERNAL_SERVER_ERROR:
            kolmogorov_repo.register_request_err(register_id)
            logger.error("Can't create banner link: %s", str(error))
            return JSONResponse(
                content={
                    "error": "Failed to generate banner link",
                    "error_reason": str(error),
                },
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        banner_id, banner_link = val, str(error)

        # Обновление таблицы
        is_outer = False
        is_technical = False
        is_deleted = False
        if is_test:
            is_deleted = True
        if str(line["partner_type"]).strip() == "+":
            is_outer = True
        if str(line["is_technical"]).lower().strip() == "да":
            is_technical = True
        try:
            banner_repo.update_db(
                banner_id=banner_id,
                banner_link=banner_link,
                title=(line["description"]).strip(),
                publication_type=(line["publication_type"]).strip(),
                is_outer=is_outer,
                channel=(line["channel"]).strip(),
                link=(line["link"]).strip(),
                is_technical=is_technical,
                partner=(line["partner"].strip()),
                is_deleted=is_deleted,
            )
        except psycopg2.errors.UniqueViolation:
            pass
        except Exception as e:
            kolmogorov_repo.register_request_err(register_id)
            logger.error("Can't update banner link: %s", str(e))
            return JSONResponse(
                content={
                    "error": "Failed to generate banner link",
                    "error_reason": str(e),
                },
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            )
        banner_links.append(banner_link)

    df["banner_links"] = banner_links
    kolmogorov_repo.register_request_succeed(register_id, df["banner_links"].to_string())
    return JSONResponse(content={"file": df.to_dict(orient="records")}, status_code=status.HTTP_200_OK)
