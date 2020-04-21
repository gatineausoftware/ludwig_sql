from pytz import timezone, utc
from datetime import datetime, timedelta
import pandas as pd
import numpy as np


from new_api import add_entity, create_new_entity_set, add_aggregation_features, get_training_df, get_prediction_df, list_features, get_event_training_df



agent_entity = {"name": "agent", "table": "agent", "type": "primary", "index": "agent_id", "time": {"field": "effective_date", "type": "effective_date"}}


add_entity(agent_entity)



acxiom_entity = {"name": "acxiom", "table": "agent_acxiom", "type": "primary", "index": "agent_id", "time": {"field": "date", "type": "effective_date"}}

add_entity(acxiom_entity)




comission_events = {"name": "agent_commission", "table": "agent_sales", "type": "event", 'index': "id", "time": {"field": "date", "type": "event"}}


add_entity(comission_events)




relationships =  [
            ("one_to_one", {"name": "agent", "index": "agent_id"}, {"name": "acxiom", "index": "agent_id"}),
            ("one_to_many", {"name": "agent", "index": "agent_id"}, {"name": "agent_commission",  "index": "agent_id"})
        ]





create_new_entity_set(name="nyl_agents", entities=["agent", "acxiom", "agent_commission"], relationships=relationships)



agent_commission_features = {"total_sales": {"feature": "amount","function":"sum", "name": "total_sales", "time_window": "full_history"},
                             "max_sales":  {"feature": "amount", "function":"max", "name": "max_sales", "time_window": "full_history"},
                             "total_num_sales":   {"feature": "id", "function": "count", "name": "total_num_sales", "time_window": "full_history"}
                            }


add_aggregation_features("agent_commission", agent_commission_features)


f = list_features("nyl_agents")

print(f)



eol = {"pk": "agent_id", "observation_date": "observation_time", "label": "prediction"}


features = {"entity_set": "nyl_agents",
            "target_entity": "agent",
            "features": {
                         "agent": ["feature_1", "feature_2", "feature_6"],
                         "acxiom": ["zipcode", "num_household"],
                         "agent_commission": ["total_sales", "max_sales"]

                    },
            "observations": {"type": "eol", "eol": eol, "data": None}
            }







#df = get_training_df(features)

#print(df)

#df = get_prediction_df(features)

#print(df)




days = [datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=utc) \
        - timedelta(day * 7) for day in range(3*52)][::-1]

agents = [1001, 1002, 1003, 1004, 1005]


eol_df = pd.DataFrame(
    {
        "id": [1000 + x for x in range(len(days) * len(agents))],
        "date": [day for day in days for agent in agents],
        "prediction": [np.random.rand() * 100 for _ in range(len(days) * len(agents))],

    }
)

eol = {"pk": "id", "observation_date": "date", "label": "prediction"}

features = {"entity_set": "nyl_agents",
            "target_entity": "agent_commission",
            "features": {
                         "agent": ["feature_1", "feature_2", "feature_6"],
                         "acxiom": ["zipcode", "num_household"],
                         "agent_commission": ["amount", "date", "feature_2", "feature_3", "total_sales", "max_sales"]
                         #"agent_commission": ["amount", "date", "feature_2", "feature_3"]

                    },
            "observations": {"type": "el", "eol": eol, "data": eol_df}
            }



df = get_event_training_df(features)

print(df)

#df = get_prediction_df(features)

#print(df)











