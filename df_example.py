from pytz import timezone, utc
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


from feature_store import add_entity_df, get_prediction_data_set, get_training_data_set, get_prediction_features



days = [datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=utc) \
        - timedelta(day) for day in range(3)][::-1]

customers = [1001, 1002, 1003, 1004, 1005]


df = pd.DataFrame(
    {
        "datetime": [day for day in days for customer in customers],
        "customer_id": [customer for day in days for customer in customers],
        "daily_transactions": [np.random.rand() * 10 for _ in range(len(days) * len(customers))],
        "total_transactions": [np.random.randint(100) for _ in range(len(days) * len(customers))],
    }
)

add_entity_df(df, entity_set="customers", name="customer_transactions", entity_id="customer_id", effective_date="datetime")




#generate training data set

days = [datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=utc) \
        - timedelta(day) for day in range(2)][::-1]

customers = [1001, 1002, 1003, 1004, 1005]


customer_eol = pd.DataFrame(
    {
        "observation_time": [day for day in days for customer in customers],
        "customer_id": [customer for day in days for customer in customers],
        "prediction": [np.random.rand()  for _ in range(len(days) * len(customers))],

    }
)


df = get_training_data_set("customers", {"pk": "customer_id", "observation_date": "observation_time", "label": "prediction"}, customer_eol)

print(df)



online_features = get_prediction_features(entity_set="customers", entity_name="customer_transactions",
    features=[
        "daily_transactions",
        "total_transactions",
    ],
    entity_rows=[1001, 1002, 1004]

)

print(online_features)
