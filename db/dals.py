from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from sqlalchemy import and_
from sqlalchemy import desc

from db.models import Url
from db.models import Metrics
from db.models import Query
from db.models import MetricsQuery


###########################################################
# BLOCK FOR INTERACTION WITH DATABASE IN BUSINESS CONTEXT #
###########################################################


class UrlDAL:
    """Data Access Layer for operating user info"""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def add_new_urls(
            self,
            add_values
    ):
        self.db_session.add_all(add_values)
        await self.db_session.flush()
        return

    async def get_urls_with_pagination(self, page, per_page, date_start, date_end):
        sub = select(Url).offset(page).limit(
            per_page).subquery()
        query = select(Metrics.date, Metrics.position, Metrics.clicks, Metrics.impression, Metrics.ctr, sub).join(sub,
                                                                                                                  Metrics.url == sub.c.url).group_by(
            sub.c.url,
            Metrics.date,
            Metrics.position,
            Metrics.clicks,
            Metrics.impression,
            Metrics.ctr,
        ).having(and_(Metrics.date <= date_end, Metrics.date >= date_start))
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row

    async def get_urls_with_pagination_and_like(self, page, per_page, date_start, date_end, search_text):
        sub = select(Url).filter(Url.url.like(f"%{search_text.strip()}%")).offset(page).limit(per_page).subquery()
        query = select(Metrics.date, Metrics.position, Metrics.clicks, Metrics.impression, Metrics.ctr, sub).join(sub,
                                                                                                                  Metrics.url == sub.c.url).group_by(
            sub.c.url,
            Metrics.date,
            Metrics.position,
            Metrics.clicks,
            Metrics.impression,
            Metrics.ctr,
        ).having(and_(Metrics.date <= date_end, Metrics.date >= date_start))
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row

    async def get_urls_with_pagination_sort(self, page, per_page, date_start, date_end, sort_desc):
        if sort_desc:
            sub = select(Url).order_by(desc(Url.url)).offset(page).limit(
                per_page).subquery()
        else:
            sub = select(Url).order_by(Url.url).offset(page).limit(
                per_page).subquery()
        query = select(Metrics.date, Metrics.position, Metrics.clicks, Metrics.impression, Metrics.ctr, sub).join(sub,
                                                                                                                  Metrics.url == sub.c.url).group_by(
            sub.c.url,
            Metrics.date,
            Metrics.position,
            Metrics.clicks,
            Metrics.impression,
            Metrics.ctr,
        ).having(and_(Metrics.date <= date_end, Metrics.date >= date_start))
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row

    async def get_urls_with_pagination_and_like_sort(self, page, per_page, date_start, date_end, search_text,
                                                     sort_desc):
        if sort_desc:
            sub = select(Url).filter(Url.url.like(f"%{search_text.strip()}%")).order_by(desc(Url.url)).offset(
                page).limit(per_page).subquery()
        else:
            sub = select(Url).filter(Url.url.like(f"%{search_text.strip()}%")).order_by(Url.url).offset(page).limit(
                per_page).subquery()
        query = select(Metrics.date, Metrics.position, Metrics.clicks, Metrics.impression, Metrics.ctr, sub).join(sub,
                                                                                                                  Metrics.url == sub.c.url).group_by(
            sub.c.url,
            Metrics.date,
            Metrics.position,
            Metrics.clicks,
            Metrics.impression,
            Metrics.ctr,
        ).having(and_(Metrics.date <= date_end, Metrics.date >= date_start))
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row


class MetricDAL:
    """Data Access Layer for operating user info"""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def add_new_metrics(
            self,
            add_values
    ):
        self.db_session.add_all(add_values)
        await self.db_session.flush()
        return


class QueryDAL:
    """Data Access Layer for operating user info"""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def add_new_urls(
            self,
            add_values
    ):
        self.db_session.add_all(add_values)
        await self.db_session.flush()
        return

    async def get_urls_with_pagination(self, page, per_page, date_start, date_end):
        sub = select(Query).offset(page).limit(
            per_page).subquery()
        query = select(MetricsQuery.date, MetricsQuery.position, MetricsQuery.clicks, MetricsQuery.impression,
                       MetricsQuery.ctr, sub).join(sub,
                                                   MetricsQuery.query == sub.c.query).group_by(
            sub.c.query,
            MetricsQuery.date,
            MetricsQuery.position,
            MetricsQuery.clicks,
            MetricsQuery.impression,
            MetricsQuery.ctr,
        ).having(and_(MetricsQuery.date <= date_end, MetricsQuery.date >= date_start))
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row

    async def get_urls_with_pagination_and_like(self, page, per_page, date_start, date_end, search_text):
        sub = select(Query).filter(Query.query.like(f"%{search_text.strip()}%")).offset(page).limit(per_page).subquery()
        query = select(MetricsQuery.date, MetricsQuery.position, MetricsQuery.clicks, MetricsQuery.impression,
                       MetricsQuery.ctr, sub).join(sub,
                                                   MetricsQuery.query == sub.c.query).group_by(
            sub.c.query,
            MetricsQuery.date,
            MetricsQuery.position,
            MetricsQuery.clicks,
            MetricsQuery.impression,
            MetricsQuery.ctr,
        ).having(and_(MetricsQuery.date <= date_end, MetricsQuery.date >= date_start))
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row

    async def get_urls_with_pagination_sort(self, page, per_page, date_start, date_end, sort_desc):
        if sort_desc:
            sub = select(Query).order_by(desc(Query.query)).offset(page).limit(
                per_page).subquery()
        else:
            sub = select(Query).order_by(Query.query).offset(page).limit(
                per_page).subquery()
        query = select(MetricsQuery.date, MetricsQuery.position, MetricsQuery.clicks, MetricsQuery.impression,
                       MetricsQuery.ctr, sub).join(sub,
                                                   MetricsQuery.query == sub.c.query).group_by(
            sub.c.query,
            MetricsQuery.date,
            MetricsQuery.position,
            MetricsQuery.clicks,
            MetricsQuery.impression,
            MetricsQuery.ctr,
        ).having(and_(MetricsQuery.date <= date_end, MetricsQuery.date >= date_start))
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row

    async def get_urls_with_pagination_and_like_sort(self, page, per_page, date_start, date_end, search_text,
                                                     sort_desc):
        if sort_desc:
            sub = select(Query).filter(Query.query.like(f"%{search_text.strip()}%")).order_by(desc(Query.query)).offset(
                page).limit(per_page).subquery()
        else:
            sub = select(Query).filter(Query.query.like(f"%{search_text.strip()}%")).order_by(Query.query).offset(
                page).limit(
                per_page).subquery()
        query = select(MetricsQuery.date, MetricsQuery.position, MetricsQuery.clicks, MetricsQuery.impression,
                       MetricsQuery.ctr, sub).join(sub,
                                                   MetricsQuery.query == sub.c.query).group_by(
            sub.c.query,
            MetricsQuery.date,
            MetricsQuery.position,
            MetricsQuery.clicks,
            MetricsQuery.impression,
            MetricsQuery.ctr,
        ).having(and_(MetricsQuery.date <= date_end, MetricsQuery.date >= date_start))
        res = await self.db_session.execute(query)
        product_row = res.fetchall()
        if len(product_row) != 0:
            return product_row


class MetricQueryDAL:
    """Data Access Layer for operating user info"""

    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def add_new_metrics(
            self,
            add_values
    ):
        self.db_session.add_all(add_values)
        await self.db_session.flush()
        return
