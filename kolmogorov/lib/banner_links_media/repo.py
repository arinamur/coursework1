from typing import Final

from lib.data_source import AnalyticsDB


class AnalyticsBannerLinks:
    def __init__(self, db_session: AnalyticsDB) -> None:
        self._db_session: Final = db_session

    def update_db(
        self,
        banner_id: int,
        banner_link: str,
        title: str,
        publication_type: str,
        is_outer: bool,
        channel: str,
        link: str,
        is_technical: bool,
        partner: str,
        is_deleted: bool,
    ) -> None:
        with self._db_session.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                insert into analytics.banner_links_media(
                    banner_id, banner_link, title, publication_type, is_outer, channel,
                    link, is_technical, partner, is_deleted
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    banner_id,
                    banner_link,
                    title,
                    publication_type,
                    is_outer,
                    channel,
                    link,
                    is_technical,
                    partner,
                    is_deleted,
                ),
            )
