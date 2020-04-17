from pytz import timezone, utc
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


from feature_store import add_entity_df, get_prediction_data_set, get_training_data_set, add_child_df, add_child_features



days = [datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=utc) \
        - timedelta(day * 365) for day in range(3)][::-1]

agents = [1001, 1002, 1003, 1004, 1005]


df = pd.DataFrame(
    {
        "effective_date": [day for day in days for agent in agents],
        "agent_id": [agent for day in days for agent in agents],
        "feature_1": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
        "feature_2": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
        "feature_3": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
        "feature_4": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
        "feature_5": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
        "feature_6": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
        "feature_7": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
        "feature_8": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
    }
)

add_entity_df(df=df, entity_set="nyl_agents", name="agent_survey", entity_id="agent_id", effective_date="effective_date")






days = [datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=utc) \
        - timedelta(day * 365) for day in range(3)][::-1]

agents = [1001, 1002, 1003, 1004, 1005]


df = pd.DataFrame(
    {
        "effective_date": [day for day in days for agent in agents],
        "agent_id": [agent for day in days for agent in agents],
        "axciom_feature_1": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],
        "axciom_feature_2": [np.random.rand() * 10 for _ in range(len(days) * len(agents))],

    }
)

add_entity_df(df, entity_set="nyl_agents", name="agent_axciom", entity_id="agent_id", effective_date="effective_date")



#other data

days = [datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=utc) \
        - timedelta(day * 7) for day in range(3*52)][::-1]

agents = [1001, 1002, 1003, 1004, 1005]


df = pd.DataFrame(
    {
        "id": [1000 + x for x in range(len(days) * len(agents))],
        "date": [day for day in days for agent in agents],
        "agent_id": [agent for day in days for agent in agents],
        "feature_1": [np.random.rand() * 100 for _ in range(len(days) * len(agents))],
        "feature_2": [np.random.rand() * 100  for _ in range(len(days) * len(agents))],
        "feature_3": [np.random.rand() * 100 for _ in range(len(days) * len(agents))],

    }
)



add_child_df(df=df, entity_set="nyl_agents", name="agent_sales", entity_id="id", parent_id="agent_id", date="date")



agent_sales_aggregation_features = [{"feature": "feature_1","function":"sum", "name": "total_sales", "time_window": "full_history"}, {"feature": "feature_1", "function":"max", "name": "max_sales", "time_window": "full_history"}]


add_child_features("nyl_agents", "agent_sales", agent_sales_aggregation_features)



#generate training data set


days = [datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=utc) \
        - timedelta(day * 365) for day in range(2)][::-1]

agents = [1001, 1002, 1003, 1004, 1005]


customer_eol = pd.DataFrame(
    {
        "observation_time": [day for day in days for customer in agents],
        "agent_id": [customer for day in days for customer in agents],
        "prediction": [np.random.rand()  for _ in range(len(days) * len(agents))],

    }
)


df = get_training_data_set("nyl_agents", {"pk": "agent_id", "observation_date": "observation_time", "label": "prediction"}, customer_eol)

print(df)

print(df.columns)


#generate prediction data set for batch...


df = get_prediction_data_set(entity_set="nyl_agents", master_entity="agent_survey")

print(df)


#generate prediciton data set for sepcific entitis...
