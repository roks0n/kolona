import logging


def get_log(name: str):
    return logging.getLogger(f"kolona.{name}")
