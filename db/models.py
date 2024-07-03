from datetime import datetime
from sqlalchemy import Float
from sqlalchemy import Column
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import DateTime
from sqlalchemy.orm import declarative_base

##############################
# BLOCK WITH DATABASE MODELS #
##############################

Base = declarative_base()


class Url(Base):
    __tablename__ = "url"

    url = Column(String, nullable=False, unique=True, primary_key=True)


class Metrics(Base):
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    url = Column(String, nullable=False)
    date = Column(DateTime, nullable=False, default=datetime.now)
    position = Column(Float, nullable=False)
    ctr = Column(Float, nullable=False)
    impression = Column(Float, nullable=False)
    demand = Column(Float, nullable=False)
    clicks = Column(Float, nullable=False)


class Query(Base):
    __tablename__ = "query"

    query = Column(String, nullable=False, unique=True, primary_key=True)


class MetricsQuery(Base):
    __tablename__ = "metrics_query"

    id = Column(Integer, primary_key=True, autoincrement=True)
    query = Column(String, nullable=False)
    date = Column(DateTime, nullable=False, default=datetime.now)
    position = Column(Float, nullable=False)
    ctr = Column(Float, nullable=False)
    impression = Column(Float, nullable=False)
    demand = Column(Float, nullable=False)
    clicks = Column(Float, nullable=False)
