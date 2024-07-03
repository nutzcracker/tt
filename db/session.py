from typing import Generator
import config

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import sessionmaker

##############################################
# BLOCK FOR COMMON INTERACTION WITH DATABASE #
##############################################

# REAL_DATABASE_URL = f"postgresql+asyncpg://mainuser:Kirill123456@5.35.81.91:5432/products"

REAL_DATABASE_URL = f"postgresql+asyncpg://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DATABASE_NAME}"
#REAL_DATABASE_URL = f"postgresql+asyncpg://dn_true:1238xcnq&qaQWER@localhost:5432/dn_true_2"
#REAL_DATABASE_URL = f"postgresql+asyncpg://dn_true:1238xcnq&qaQWER@localhost:5432/ayshotel"

# create async engine for interaction with database
engine = create_async_engine(
    REAL_DATABASE_URL,
    future=True,
    execution_options={"isolation_level": "AUTOCOMMIT"},
)

# create session for the interaction with database
async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)


async def get_db() -> Generator:
    """Dependency for getting async session"""
    try:
        session: AsyncSession = async_session()
        yield session
    finally:
        await session.close()
