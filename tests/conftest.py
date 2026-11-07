import os,tempfile
os.environ['CHANGAN_BACKEND']='process'
os.environ['CHANGAN_DATA_DIR']=tempfile.mkdtemp(prefix='changan-tests-')
import pytest
from app import db
@pytest.fixture(autouse=True)
def fresh_db():
    db.init()
    yield
    with db.connect() as c:
        for table in ['node_incidents','alerts','events','attempts','segments','tasks','strategies','nodes']:
            c.execute('DELETE FROM '+table)
