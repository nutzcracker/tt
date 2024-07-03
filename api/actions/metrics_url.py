from db.dals import MetricDAL


async def _add_new_metrics(urls, session):
    async with session() as s:
        order_dal = MetricDAL(s)
        order_id = await order_dal.add_new_metrics(
            urls
        )
        return order_id
