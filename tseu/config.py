import os


class Config(object):
    wrds_username = os.environ.get("WRDS_USERNAME") or "iewaij"
    basedir = os.path.abspath(os.path.dirname(__file__))
