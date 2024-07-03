"""File with settings and configs for the project"""
from envparse import Env
import config

env = Env()

REAL_DATABASE_URL = env.str(
    "localhost",
    
    default="postgresql+asyncpg://{config.DB_USER}:{config.DB_PASSWORD}@{config.DB_HOST}:{config.DB_PORT}/{config.DATABASE_NAME}",
    #default="postgresql+asyncpg://dn_true:1238xcnq&qaQWER@localhost:5432/dn_temp_2",
)  # connect string for the real database
APP_PORT=8000

#REAL_DATABASE_URL = env.str(
#    "REAL_DATABASE_URL",
#    default="postgresql+asyncpg://postgres:postgres@0.0.0.0:5484/postgres",
#)  # connect string for the real database
#APP_PORT = env.int("APP_PORT", default=8888)
