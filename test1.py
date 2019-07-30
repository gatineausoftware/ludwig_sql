import sqlalchemy as db
import pandas as pd
from numpy import dtype
from ludwig.api import LudwigModel
import tarfile


engine = db.create_engine('mysql+pymysql://root:swamp@localhost:3306/ml')

connection = engine.connect()
metadata = db.MetaData()
titanic = db.Table('titanic', metadata, autoload=True, autoload_with=engine)
#print(repr(metadata.tables['titanic']))


query = db.select([titanic])
df = pd.read_sql_query(query, engine)


#fix
df['Survived'] = df['Survived'].astype('bool')
target = 'Survived'
#end fix


type_mappings = {dtype('int64'): 'numerical', dtype('float64'): 'numerical', dtype('object'): 'category', dtype('bool'): 'binary'}

input_features = []

for col in df.columns:
 if col == target:
   continue
 n = df[col].name
 t = df[col].dtype
 input_features.append({'name': n, 'type': type_mappings[t]})


model_def = {}
model_def['input_features'] = input_features
model_def['output_features'] =  [{'name': 'Survived', 'type': 'binary'}]



'''
model_def = {'input_features':
 [{'name': 'Class', 'type': 'category'},
  {'name': 'Sex', 'type': 'category'},
  {'name': 'Age',
   'type': 'numerical',
   'preprocessing': {'missing_value_strategy': 'fill_with_mean',
                    'normalization' : 'zscore'}},
  {'name': 'SibSp', 'type': 'numerical'},
  {'name': 'Parch', 'type': 'numerical'},
  {'name': 'Fare',
   'type': 'numerical',
   'preprocessing': {'missing_value_strategy': 'fill_with_mean'}},
  {'name': 'Embarked', 'type': 'category'}],
 'output_features':
 
'''

ludwig_model = LudwigModel(model_def)
train_status = ludwig_model.train(data_df=df)
ludwig_model.save('model')

with tarfile.open('titanic_model.tar.gz', 'w:gz') as tar:
    tar.add('model')