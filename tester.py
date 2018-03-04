import sqlalchemy
from sqlalchemy.engine import create_engine

E = create_engine('postgresql://test:test@docker.for.mac.host.internal:5432/test')
E.connect()



# E = create_engine('postgresql://test:test@localhost:5432/test')
# E.connect()
