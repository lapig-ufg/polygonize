from loguru import logger
from decouple import config

MONGO = config('MONGO_CONNECTION')
PG_CONNECTION = config('PG_CONNECTION')

logger.add(
    f'logs/logger.log',
    format='[{time} | {level:<6}] {module}.{function}:{line} {message}',
    rotation='500 MB',
)
logger.add(
    f'logs/logger_error.log',
    format='[{time} | {level:<6}] {module}.{function}:{line} {message}',
    level='WARNING',
    rotation='500 MB',
)
