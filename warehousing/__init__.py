from pathlib import Path


def sql_path():
    return Path(__file__).parent.absolute() / 'sql'
