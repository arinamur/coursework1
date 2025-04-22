import json
import logging

import pandas as pd
from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from lib.skill_executions.banner_links_media.generation import generate_link
from lib.skill_executions.banner_links_media.repo import AnalyticsBannerLinks
from lib.skill_executions.banner_links_media.types import EnumSkillError, ErrorCode
from lib.skill_executions.banner_links_media.validation import validation
from src.skill_executions.repo import SkillExecutionsRepo

logger = logging.getLogger(__name__)

banner_link_media_router = APIRouter()


@banner_link_media_router.post("/bannerLinksMedia")
def banner_links_media(file: str, request: Request) -> JSONResponse:
    kolmogorov_repo = SkillExecutionsRepo(request.app.state.db_kolmogorov)

    try:
        df = pd.DataFrame(json.loads(file)).dropna(how="all").fillna("")

    except Exception as e:
        err = EnumSkillError(ErrorCode.CANT_PARSE_FILE, str(e))
        logger.error("%s: %s", err.code.name, err)
        return err.to_response()

    data = {"records": df.to_dict(orient="records")}
    register_id = kolmogorov_repo.register_running_request("BannerLinksMediaReport", data)
    banner_repo = AnalyticsBannerLinks(request.app.state.db_analytics)

    # Валидация данных
    error = validation(df)
    if error:
        kolmogorov_repo.register_request_err(register_id)
        logger.error("%s: %s", err.code.name, err)
        return error.to_response()

    # Генерация баннерных ссылок
    is_test = False
    banner_links = []

    for _, line in df.iterrows():
        val, error = generate_link(line, is_test)

        if val == status.HTTP_500_INTERNAL_SERVER_ERROR:
            kolmogorov_repo.register_request_err(register_id)
            logger.error("%s: %s", ErrorCode.BANNER_GENERATION.name, error)
            return EnumSkillError(ErrorCode.BANNER_GENERATION, str(error)).to_response()

        banner_id, banner_link = val, str(error)

        # Обновление таблицы
        try:
            is_outer = str(line["partner_type"]).strip() == "+"
            is_technical = str(line["is_technical"]).lower().strip() == "да"
            is_deleted = is_test

            banner_repo.update_db(
                banner_id=banner_id,
                banner_link=banner_link,
                title=line["description"].strip(),
                publication_type=line["publication_type"].strip(),
                is_outer=is_outer,
                channel=line["channel"].strip(),
                link=line["link"].strip(),
                is_technical=is_technical,
                partner=line["partner"].strip(),
                is_deleted=is_deleted,
            )
        except Exception as e:
            kolmogorov_repo.register_request_err(register_id)
            logger.error("Can't update banner link: %s", str(e))
            return EnumSkillError(ErrorCode.DB_UPDATE_FAILED, str(e)).to_response()

        banner_links.append(banner_link)

    df["banner_links"] = banner_links
    kolmogorov_repo.register_request_succeed(register_id, df["banner_links"].to_string())
    return JSONResponse(content={"file": df.to_dict(orient="records")}, status_code=status.HTTP_200_OK)
