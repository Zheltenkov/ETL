import os


class Settings:
    server = os.environ.get('GP_HOST', 'localhost:5432')
    database = os.environ.get('GP_DATABASE', 'postgres')
    user = os.environ.get('GP_USER', 'postgres')
    pswd = os.environ.get('GP_PASS', 'test!123')
