import sqlalchemy as db
import pandas as pd



engine = db.create_engine('mysql+pymysql://root:swamp@localhost:3306/ml')

connection = engine.connect()

def get_stuf():

    df1 = pd.read_sql("SELECT agent_eol.agent_id, obvservation_date, label, b.feature1, b.feature2 \
        from agent_eol \
        left join agent b \
        on agent_eol.agent_id  = b.agent_id \
        AND b.effective_date = (select max(effective_date) from agent b2 where b2.agent_id = agent_eol.agent_id and b2.effective_date <= obvservation_date)", connection)

    print(df1)


    df2 = pd.read_sql("select agent_eol.agent_id, agent_eol.obvservation_date, count(distinct complaints_history.complaint_id) as num_complaints, max(complaints_history.complaint_feature) as max_f from agent_eol join complaints_history on \
                        agent_eol.agent_id = complaints_history.agent_id and \
                        agent_eol.obvservation_date >= complaints_history.complaint_date \
                        group by agent_eol.agent_id, agent_eol.obvservation_date", connection)


    pd.set_option('display.expand_frame_repr', False)

    print(df2)


    df3 = pd.read_sql("SELECT agent_eol.agent_id, agent_eol.obvservation_date, label, b.feature1, c.num_complaints, c.max_f \
    from agent_eol \
    left join agent b \
    on agent_eol.agent_id  = b.agent_id \
    AND b.effective_date = (select max(effective_date) from agent b2 where b2.agent_id = agent_eol.agent_id and b2.effective_date <= obvservation_date) \
    left join \
    (select agent_eol.agent_id, agent_eol.obvservation_date, count(distinct complaints_history.complaint_id) as num_complaints, max(complaints_history.complaint_feature) as max_f  from agent_eol join complaints_history on \
    agent_eol.agent_id = complaints_history.agent_id and \
    agent_eol.obvservation_date >= complaints_history.complaint_date \
    group by agent_eol.agent_id, agent_eol.obvservation_date) c \
    on agent_eol.agent_id = c.agent_id and agent_eol.obvservation_date = c.obvservation_date", connection)


    print(df3)



feature_store = {"child": []}

def add_entity(name, table, pk, effective_date, features):
    entity = {"name": name, "table": table, "pk": pk, "effective_date": effective_date, "features": features}
    feature_store["entity"] = entity


def add_aggregation_child(name, table, pk, parent_key, tdate, column, type):
    entity = {"name": name, "table": table, "pk": pk, "parent_key": parent_key, "date": tdate, "column": column, "type": type}
    feature_store["child"].append(entity)


def add_eol(table, pk, observation_date, label):
    entity = {"table": table, "pk": pk, "observation_date": observation_date, "label": label}
    feature_store["eol"] = entity


def get_agg_function(child):

    table = child["table"]
    column = child["column"]
    agg = child["type"]

    if agg=="count":
        return f"count(distinct {table}.{column})"
    elif agg == "max":
        return f"max({table}.{column})"



def get_training_data_set(feature_store):
    eol = feature_store["eol"]
    eol_tn = eol["table"]
    eol_pk = eol["pk"]
    eol_od = eol["observation_date"]
    eol_lb = eol["label"]


    sql1 = f"select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {eol_tn}.{eol_lb}"

    e= feature_store["entity"]
    e_tn = e["table"]
    e_pk = e["pk"]
    e_ed = e["effective_date"]
    e_fe = e["features"]

    sql2 = ""

    for feature in e_fe:
        sql2 += f", a.{feature}"


    sql3 = f" from {eol_tn} left join {e_tn} a on {eol_tn}.{eol_pk} = a.{e_pk} and \
    a.{e_ed} = (select max({e_ed}) from {e_tn} a2 where a2.{e_pk} = {eol_tn}.{eol_pk} and a2.{e_ed} <= {eol_tn}.{eol_od})"



    sql4 = ""

    for child in feature_store["child"]:
        c_nm = child["name"]
        c_tn = child["table"]
        c_pk = child["pk"]
        c_parentk = child["parent_key"]
        c_d = child["date"]

        sql4 += f" left join (select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {get_agg_function(child)} as {c_nm} from \
            {eol_tn} join {c_tn} on {eol_tn}.{eol_pk} = {c_tn}.{c_parentk} and {eol_tn}.{eol_od} >= {c_tn}.{c_d} group by \
            {eol_tn}.{eol_pk}, {eol_tn}.{eol_od} ) c on \
            {eol_tn}.{eol_pk} = c.{eol_pk} and {eol_tn}.{eol_od} = c.{eol_od}"


        sql2 += f" , c.{c_nm}"



    sql = sql1 + sql2 + sql3 + sql4

    df = pd.read_sql(sql, connection)

    return df




add_eol("agent_eol", "agent_id", "obvservation_date", "label")

add_entity("agent", "agent", "agent_id", "effective_date", ["feature1", "feature2"])

add_aggregation_child("num_complaints","complaints_history", "complaint_id", "agent_id", "complaint_date", "complaint_id", "count")

df = get_training_data_set(feature_store)

print(df)




















