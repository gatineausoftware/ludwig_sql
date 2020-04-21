import pandas as pd

from new_api import (add_entity, create_new_entity_set, add_aggregation_features, get_training_df, get_prediction_df,
add_entity_df, list_features, get_training_df_el, generate_base_model_definition)


df = pd.read_csv('titanic.csv')

passenger_entity = {"name": "passengers", "table": "passengers", "type": "primary", "index": "PassengerId"}

add_entity_df(df, passenger_entity)



create_new_entity_set(name="titanic", entities=["passengers"], relationships=[])




eol_df = df[["PassengerId", "Survived"]]


eol = {"pk": "PassengerId", "label": "Survived"}

features = {"entity_set": "titanic",
            "target_entity": "passengers",
            "features": {
                "passengers": ["Pclass","Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked"]


            },
            "observations": {"type": "el", "eol": eol, "data": eol_df}

            }
training_df = get_training_df_el(features)


x = generate_base_model_definition(features)

print(x)

