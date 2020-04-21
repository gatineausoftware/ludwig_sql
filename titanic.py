import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pytz import timezone, utc


from new_api import (add_entity, create_new_entity_set, add_aggregation_features, get_training_df, get_prediction_df,
add_entity_df, list_features, get_training_df_el, generate_base_model_definition)


passenger_df = pd.read_csv('titanic.csv')

passenger_entity = {"name": "passengers", "table": "passengers", "type": "primary", "index": "PassengerId"}

add_entity_df(passenger_df, passenger_entity)





passengers = [i for i in range(1, 892)]

days = [datetime(2012,4,10).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=utc) \
        + timedelta(day) for day in range(4)][::-1]



passenger_activity_df = pd.DataFrame(
    {
        "id": [i for i in range(len(passengers) * len(days))],
        "PassengerId": [passenger for day in days for passenger in passengers],
        "date": [day for day in days for passenger in passengers],
        "buffet": [np.random.rand() * 10 for _ in range(len(days) * len(passengers))],
        "ballroom": [np.random.randint(10) for _ in range(len(days) * len(passengers))],
    }
)


passenger_activity = {"name": "passenger_activity", "table": "passenger_activity", "type": "event", "index": "id"}

add_entity_df(passenger_activity_df, passenger_activity)

relationships =  [
                 ("one_to_many", {"name": "passengers", "index": "PassengerId"}, {"name": "passenger_activity",  "index": "PassengerId"})
                ]

create_new_entity_set(name="titanic", entities=["passengers","passenger_activity"], relationships=relationships)


passsenger_activity_rollup = {"total_buffet": {"feature": "buffet","function":"sum", "name": "total_buffet", "time_window": "full_history"},
                             "max_buffet":  {"feature": "buffet", "function":"max", "name": "max_buffet", "time_window": "full_history"},
                             "total_ballroom":   {"feature": "ballroom", "function": "sum", "name": "total_ballroom", "time_window": "full_history"}
                            }
add_aggregation_features("passenger_activity", passsenger_activity_rollup)



eol_df = passenger_df[["PassengerId", "Survived"]]


eol = {"pk": "PassengerId", "label": "Survived"}

features = {"entity_set": "titanic",
            "target_entity": "passengers",
            "features": {
                "passengers": ["Pclass","Sex","Age","SibSp","Parch","Ticket","Fare","Cabin","Embarked"],
                "passenger_activity": ["total_buffet", "max_buffet", "total_ballroom"]

            },
            "observations": {"type": "el", "eol": eol, "data": eol_df}

            }
training_df = get_training_df_el(features)


generate_base_model_definition(features)


