import sqlalchemy as db
import pandas as pd


engine = db.create_engine('mysql+pymysql://root:swamp@localhost:3306/ml')

connection = engine.connect()
metadata = db.MetaData()
titanic = db.Table('titanic', metadata, autoload=True, autoload_with=engine)
print(repr(metadata.tables['titanic']))


query = db.select([titanic])
df = pd.read_sql_query(query, engine)
print(df.head())

for col in df.columns:
    print(df[col].name)
    print(df[col].dtype)