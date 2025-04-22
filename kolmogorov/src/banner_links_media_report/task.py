import logging
import os
import tempfile

import pandas as pd

from lib.data_source import AnalyticsDB
from lib.queries.repo import AnalyticsRepo
from lib.reports.banner_links_media_report.parsing import table_parse
from lib.reports.banner_links_media_report.repo import ReportBannerLinksMediaRepo
from lib.reports.s3_file_storage import MinioBucketResultStorage
from lib.time import TimeRange, now_time_msk, parse_time
from src.data_source import KolmogorovDB
from src.reports.api.models import BannerLinksMediaParameters, Parameters
from src.reports.base.register import TaskRegister
from src.reports.base.task import Task
from src.reports.base.types import ErrorReason, ParametersInfo, ReportsType, TaskResult, Ticket

logger = logging.getLogger(__name__)


class ReportBannerLinksMedia(Task):
    def check_parameters(
        self,
        parameters: Parameters,
        db_analytics: AnalyticsDB,
    ) -> bool:
        if not isinstance(parameters, BannerLinksMediaParameters):
            return False

        try:
            TimeRange(from_in=parse_time(parameters.from_date), to_in=parse_time(parameters.to_date))
        except Exception:
            return False

        return True

    def check_report_ready(
        self,
        parameters: Parameters,
        db_analytics: AnalyticsDB,
        db_kolmogorov: KolmogorovDB,
    ) -> ParametersInfo | None:
        if not isinstance(parameters, BannerLinksMediaParameters):
            return None

        report_path = self._get_report_path(parameters, db_kolmogorov)

        if not report_path:
            return None
        if parse_time(parameters.to_date).date() >= now_time_msk().date():  # type:ignore[union-attr]
            return None
        return ParametersInfo(
            error=None,
            result_url=report_path,
        )

    @staticmethod
    def _get_report_path(
        parameters: BannerLinksMediaParameters,
        db_kolmogorov: KolmogorovDB,
    ) -> str | None:
        task_register = TaskRegister(db_kolmogorov)
        last_report = task_register.get_last_report(ReportsType.REPORT_BANNER_LINKS_MEDIA.value, parameters)

        if not last_report:
            return None

        for report_path, _ in last_report.items():
            return report_path

        return None

    def call(
        self,
        cheops_repo: AnalyticsRepo,
        ticket: Ticket,
        parameters: Parameters,
        minio_storage: MinioBucketResultStorage,
        bucket_name: str,
    ) -> TaskResult:
        if not isinstance(parameters, BannerLinksMediaParameters):
            return TaskResult(request_result=None, error_details=ErrorReason.PARAMETERSERROR.value)

        tr = TimeRange(from_in=parse_time(parameters.from_date), to_in=parse_time(parameters.to_date))
        repo = ReportBannerLinksMediaRepo()
        with tempfile.TemporaryDirectory(suffix=f"{ticket}") as tempdir:
            try:
                rows, cols, _ = cheops_repo.perform_q(repo.get_q(tr), [])
                rows1, cols1, _ = cheops_repo.perform_q(repo.get_all_q(tr), [])
            except Exception as e:
                logger.exception("Failed to generate banner links media report data for ticket=%s: %s", ticket, e)
                return TaskResult(request_result=None, error_details=ErrorReason.CANTCALCULATE.value)

            table = pd.DataFrame(rows, columns=cols)
            table = table_parse(table)
            table["fact_publication_date"] = table["fact_publication_date"].apply(
                lambda x: parse_time(x).date().strftime("%d.%m.%Y") if pd.notna(x) else ""  # type: ignore[union-attr]
            )
            table = table.sort_values(by="fact_publication_date")
            table = table.rename(
                columns={
                    "link": "Ссылка",
                    "title": "Название публикации",
                    "publication_type": "Тип публикации",
                    "channel": "Канал",
                    "partner_type": "Тип партнёра",
                    "partner": "Партнёр",
                    "fact_publication_date": "Фактическая дата публикации",
                    "clicks": "Переходы",
                    "regs": "Регистрации",
                    "active": "Активные",
                }
            )
            final_str = pd.DataFrame(rows1, columns=cols1)
            table = pd.concat([table, final_str], ignore_index=True, axis=0)
            table["Переходы -> Регистрации"] = round((table["Регистрации"] / table["Переходы"] * 100).fillna(0))
            table["Регистрации -> Активные"] = round((table["Активные"] / table["Регистрации"] * 100).fillna(0))
            try:
                report_file = os.path.join(tempdir, f"report_{ticket}.csv")
                table.to_csv(report_file, index=False)
                bucket_path, result_path = minio_storage.save_result(
                    bucket_name=bucket_name,
                    result_uuid=ticket,
                    result=report_file,
                    report_path="rob-analytics/reports/banner_links_media",
                )
                request_result = f"{bucket_path}/{result_path}"
                return TaskResult(request_result=request_result, error_details=None)
            except Exception as exc:
                logger.exception("Failed to save report for ticket=%s: %s", ticket, exc)
                return TaskResult(request_result=None, error_details=ErrorReason.CANTUPLOAD.value)
