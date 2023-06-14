from typing import List

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from adapters.database.settings import Settings

from adapters.database.tables.adb.cdm import (Metrica1, Metrica23, Metrica4)
from adapters.database.repositories.adb.etl_kaznacheistvo import (EtlKaznacheistvoRepo)

if __name__ == "__main__":

    print(str("SQLALCHEMY VERSION: " + sqlalchemy.__version__ + "\n"))
    url = 'postgresql+psycopg2://{}:{}@{}/{}'.format(
        Settings.user, Settings.pswd, Settings.server, Settings.database
    )
    engine = create_engine(url).connect()
    session = Session(bind=engine)

    kaznacheistvo_repo: EtlKaznacheistvoRepo = EtlKaznacheistvoRepo()
    try:
        metrica_4: List[Metrica4] = kaznacheistvo_repo.getMetrica4(session)
        if len(metrica_4) > 0:
            kaznacheistvo_repo.processMetrica4(session, metrica_4)
        else:
            print("\nEmpty Metrica4 input. Closing processing.\n")
        metrica_23: List[Metrica23] = kaznacheistvo_repo.getMetrica23(session)
        if len(metrica_23) > 0:
            kaznacheistvo_repo.processMetrica23(session, metrica_23)
        else:
            print("\nEmpty Metrica23 input. Closing processing.\n")
        metrica_1: List[Metrica1] = kaznacheistvo_repo.getMetrica1(session)
        if len(metrica_1) > 0:
            kaznacheistvo_repo.processMetrica1(session, metrica_1)
        else:
            print("\nEmpty Metrica1 input. Closing processing.\n")
    except Exception as e:
        print("\nERROR: " + str(e))
    finally:
        print("\nTask completed.")
        session.close()
