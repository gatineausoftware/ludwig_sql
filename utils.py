
from numpy import dtype
import sqlalchemy as db
import pandas as pd
import tarfile
import yaml

type_mappings = {dtype('int64'): 'numerical', dtype('float64'): 'numerical', dtype('object'): 'category', dtype('bool'): 'binary'}


def get_model_definition(df, target):
    # fix

    # target = 'Survived'
    # end fix

    input_features = []

    for col in df.columns:
        if col == target:
            continue
        n = str(df[col].name)
        t = df[col].dtype
        input_features.append({'name': n, 'type': type_mappings[t]})


    model_def = {}
    model_def['input_features'] = input_features
    model_def['output_features'] =  [{'name': 'Survived', 'type': 'binary'}]

    return model_def

def generate_base_model_definition(df, target):
    model_def = get_model_definition(df, target)
    #some versioning would be good
    with open('model_definition_01.yaml', 'w') as yaml_file:
        yaml.dump(model_def, yaml_file, default_flow_style=False)

    return model_def



def get_training_data_set(url, table):
    engine = db.create_engine(url)
    metadata = db.MetaData()
    titanic = db.Table(table, metadata, autoload=True, autoload_with=engine)

    query = db.select([titanic])
    df = pd.read_sql_query(query, engine)
    #cheating...
    df['Survived'] = df['Survived'].astype('bool')
    return df


def deploy_model_to_fastscore(ludwig_model):
    ludwig_model.save('model')

    with tarfile.open('titanic_model.tar.gz', 'w:gz') as tar:
        tar.add('model')

    #need to use fastscore sdk to send model to fastscore