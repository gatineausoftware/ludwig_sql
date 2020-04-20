from new_api import add_entity, create_new_entity_set, add_aggregation_features, get_training_df, get_prediction_df, list_features



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







df = get_training_df(features)

print(df)

df = get_prediction_df(features)

print(df)

eol_df = []

features = {"entity_set": "nyl_agents",
            "target_entity": "agent_commission",
            "features": [
                            ("agent", "feature_1"),
                            ("agent", "feature_2"),
                            ("agent", "feature_6"),
                            ("acxiom", "zipcode"),
                            ("acxiom", "num_household"),
                            ("agent_commission", "amount"),
                            ("agent_commission", "total_sales")
            ]}



eol = {"pk": "id", "observation_date": "observation_time", "label": "prediction"}

#df = get_training_df(features, eol, eol_df)

#print(df)

#df = get_prediction_df(features)

#print(df)











