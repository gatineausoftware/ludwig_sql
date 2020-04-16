from feature_store import add_entity_set, add_eol, add_entity, add_aggregation_child, get_training_data_set, get_prediction_data_set



add_entity_set("agent_es")


add_entity("agent_es", "agent", "agent", "agent_id", "effective_date", ["feature1", "feature2"])

add_aggregation_child("agent_es", "num_complaints","complaints_history", "complaint_id", "agent_id", "complaint_date", "complaint_id", "count")
add_aggregation_child("agent_es", "max_feature","complaints_history", "complaint_id", "agent_id", "complaint_date", "complaint_feature", "max")


df = get_training_data_set('agent_es', {'table': 'agent_eol', "pk": "agent_id", "observation_date": "obvservation_date", "label": "label"})

print(df)



df = get_prediction_data_set('agent_es')

print(df)
