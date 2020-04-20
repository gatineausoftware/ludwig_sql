

from feature_store import add_entity_set, add_entity_table, get_training_data_set, get_prediction_data_set, get_features, add_child_table, add_child_features




add_entity_set(entity_set="agent_es")


add_entity_table(entity_set="agent_es", name="agent", table="agent", pk="agent_id", effective_date="effective_date", features=["feature1", "feature2"])


add_child_table(entity_set="agent_es", table="complaints_history", entity_id="complaint_id", parent_id="agent_id", date="complaint_date")


agent_aggregation_features = [{"feature": "complaint_id","function":"count", "name": "num_complaints", "time_window": "full_history"}, {"feature": "complaint_feature", "function":"max", "name": "max_feature", "time_window": "full_history"}]


add_child_features("agent_es", "complaints_history", agent_aggregation_features)



f = get_features("agent_es")

print(f)

df = get_training_data_set('agent_es', {'table': 'agent_eol', "pk": "agent_id", "observation_date": "obvservation_date", "label": "label"})

print(df)




df = get_prediction_data_set('agent_es', root_entity="agent")

print(df)


