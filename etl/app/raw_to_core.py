from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from adapters.database.settings import Settings
from adapters.database.repositories.adb.raw_to_core import RawToCoreRepo

if __name__ == "__main__":
    url = 'postgresql+psycopg2://{}:{}@{}/{}'.format(
        Settings.user, Settings.pswd, Settings.server, Settings.database
    )
    engine = create_engine(url)
    session = Session(bind=engine)
    rawToCoreRepo = RawToCoreRepo()
    try:
        rawToCoreRepo.processReguh(session)
        rawToCoreRepo.processRegup(session)
        rawToCoreRepo.processKblk(session)
        rawToCoreRepo.processBseg(session)
        rawToCoreRepo.processBkpf(session)
        rawToCoreRepo.processVtbfha(session)
        rawToCoreRepo.processVtbfhapo(session)
        rawToCoreRepo.processVtbfhazu(session)
        rawToCoreRepo.processT012(session)
        rawToCoreRepo.processT001(session)
        rawToCoreRepo.processBut000(session)
        rawToCoreRepo.processDraw(session)
        rawToCoreRepo.processVtbAsgnLimit(session)
        rawToCoreRepo.processTracvAccitem(session)
        rawToCoreRepo.processKblp(session)
    except Exception as e:
        print("ERROR: " + str(e))
    finally:
        print("Task completed.")
        session.close()
