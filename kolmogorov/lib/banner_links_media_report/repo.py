from typing import final

from lib.time import TimeRange


@final
class ReportBannerLinksMediaRepo:
    def __init__(self) -> None:
        pass

    def get_q(self, tr: TimeRange) -> str:
        q = f"""with excluded as (
            select
                user_id
            from analytics.excluded_course_students
            union
            select
                user_id
            from stat.excluded_users
        ),
        clicked_banners as (
            select
                distinct banner_id
            from noopolis.metrics_banner_click
            where {tr.as_sql('time_follow')}
        ),
        banners_new as (
            select
                blm.banner_id,
                blm.link,
                blm.title,
                blm.publication_type,
                blm.channel,
                blm.partner,
                blm.is_outer as partner_type,
                blm.time_created
            from analytics.banner_links_media blm
            where banner_id in (select banner_id from clicked_banners)
              and not blm.is_deleted
        ),
        banners_old as (
            select
                mb.id as banner_id,
                mb.link_reference as link,
                mb.description as title,
                null::text as publication_type,
                null::text as channel,
                null::text as partner,
                null::boolean as partner_type,
                mb.time_created
            from noopolis.metrics_banner mb
            where id in (select banner_id from clicked_banners)
              and mb.type = 'link'
              and mb.description like 'Соцсеть:%%'
        ),
        banners AS (
            select *
            from banners_new
            union all
            select o.*
            from banners_old o
            where not exists (
                select 1
                from banners_new n
                where n.banner_id = o.banner_id
            )
        ),
        banner_clicks_ordered as (
            select
                m.banner_id,
                m.time_follow,
                row_number() over (partition by m.banner_id order by m.time_follow asc) as rnk
            from noopolis.metrics_banner_click m
            where m.banner_id in (select banner_id from banners)
              and {tr.as_sql('m.time_follow')}
        ),
        fact_pub_date as (
            select
                banner_id,
                time_follow as fact_publication_date
            from banner_clicks_ordered
            where rnk = 5
        ),
        clicks as (
            select
                coalesce(user_id, 0) as user_id,
                banner_id,
                time_follow
            from noopolis.metrics_banner_click
            where banner_id in (select banner_id from banners)
              and {tr.as_sql('time_follow')}
              and coalesce(user_id, 0) not in (select user_id from excluded)
        ),
        cnt_clicks as (
            select
                banner_id,
                count(*) as clicks
            from clicks
            group by banner_id
        ),
        regs as (
            select
                ucp.user_id,
                ucp.course_id,
                ucp.id,
                c.banner_id
            from clicks c
                join noopolis.user_course_progress ucp
                  on c.user_id = ucp.user_id
            where ucp.time_created > c.time_follow
              and (ucp.time_created - c.time_follow) <= interval '30 minutes'
              and ucp.user_id not in (select user_id from excluded)
        ),
        cnt_regs as (
            select
                banner_id,
                count(distinct id) as regs
            from regs
            group by banner_id
        ),
        active as (
            select
                ump.user_id,
                ump.course_id,
                ump.id,
                r.banner_id
            from regs r
                join noopolis.user_module_progress ump
                  on ump.course_progress_id = r.id
                join noopolis.course_module cm
                  on cm.id = ump.course_module_id
            where cm.type = 'ordinary'
              and cm.level = 1
              and not cm.is_deleted
              and not ump.is_deleted
              and ump.is_achieved = true
              and (ump.is_available or ump.time_updated is not null)
              and r.user_id not in (select user_id from excluded)
        ),
        cnt_active as (
            select
                banner_id,
                count(distinct (user_id, course_id)) as active
            from active
            group by banner_id
        )
        select
            b.banner_id as id,
            b.link,
            b.channel,
            b.partner,
            b.partner_type,
            b.publication_type,
            fp.fact_publication_date,
            b.title,
            coalesce(c.clicks, 0) as clicks,
            coalesce(r.regs, 0) as regs,
            coalesce(a.active, 0) as active
        from banners b
            left join fact_pub_date fp
              on fp.banner_id = b.banner_id
            left join cnt_clicks c
              on c.banner_id = b.banner_id
            left join cnt_regs r
              on r.banner_id = b.banner_id
            left join cnt_active a
              on a.banner_id = b.banner_id
        """  # noqa: S608
        return q

    def get_all_q(self, tr: TimeRange) -> str:
        q = f"""with excluded as (
            select
                user_id
            from analytics.excluded_course_students
            union
            select
                user_id
            from stat.excluded_users
        ),
        clicks as (
            select
                coalesce(user_id, 0) as user_id,
                time_follow
            from noopolis.metrics_banner_click
            where {tr.as_sql('time_follow')}
              and coalesce(user_id, 0) not in (select user_id from excluded)
        ),
        total_clicks as (
            select
                count(*) as total_clicks
            from clicks
        ),
        regs as (
            select
                ucp.id,
                ucp.user_id,
                ucp.course_id
            from clicks c
                join noopolis.user_course_progress ucp
                  on c.user_id = ucp.user_id
            where ucp.time_created > c.time_follow
              and (ucp.time_created - c.time_follow) <= interval '30 minutes'
              and ucp.user_id not in (select user_id from excluded)
        ),
        total_regs as (
            select
                count(distinct id) as total_regs
            from regs
        ),
        active as (
            select
                r.id,
                ump.user_id,
                ump.course_id
            from regs r
                join noopolis.user_module_progress ump
                  on ump.course_progress_id = r.id
                join noopolis.course_module cm
                  on cm.id = ump.course_module_id
            where cm.type = 'ordinary'
              and cm.level = 1
              and not cm.is_deleted
              and not ump.is_deleted
              and ump.is_achieved = true
              and (ump.is_available or ump.time_updated is not null)
              and r.user_id not in (select user_id from excluded)
        ),
        total_active as (
            select
                count(distinct id) as total_active
            from active
        )
        select
            'Итог' id,
            null as link,
            null as channel,
            null as partner,
            null as partner_type,
            null as publication_type,
            null as fact_publication_date,
            null as title,
            (select total_clicks from total_clicks) as clicks,
            (select total_regs from total_regs) as regs,
            (select total_active from total_active) as active
        """  # noqa: S608
        return q
