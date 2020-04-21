
import sqlalchemy as db
import pandas as pd
from sqlalchemy import MetaData
from numpy import dtype
import yaml



engine = db.create_engine('mysql+pymysql://root:swamp@localhost:3306/ml')

connection = engine.connect()


feature_store = {
                 "entities": {},
                 "entity_sets": {},
                 }


metadata = MetaData()


def create_new_entity_set(name, entities, relationships):
    feature_store["entity_sets"][name] = {}
    feature_store["entity_sets"][name]["entities"] = entities
    feature_store["entity_sets"][name]["relationships"] = relationships


def add_entity(entity):
    feature_store["entities"][entity["name"]] = entity
    feature_store["entities"][entity["name"]]["raw_features"] = []

    metadata.reflect(bind=engine)

    for c in metadata.tables[entity["table"]].columns:
        feature_store["entities"][entity["name"]]["raw_features"].append(c.name)





def add_entity_df(df, entity):
    try:

        frame = df.to_sql(entity["table"], connection, if_exists='replace');

    except ValueError as vx:
        print(vx)
        return

    except Exception as ex:
        print(ex)
        return

    add_entity(entity)





def add_aggregation_features(entity_name, features):
    feature_store["entities"][entity_name]["aggregation_features"] = features




def write_eol(entity_set, df):
    try:

        frame = df.to_sql(entity_set + "_eol", connection, if_exists='replace');

    except ValueError as vx:
        print(vx)
        return

    except Exception as ex:
        print(ex)
        return





def get_agg_function(table, child_feature):

    column = child_feature["feature"]
    agg = child_feature["function"]

    if agg=="count":
        return f"count(distinct {table}.{column})"
    elif agg == "max":
        return f"max({table}.{column})"
    elif agg == "sum":
        return f"sum({table}.{column})"



def get_parent_key(entity_set_name, target_name, child_name):
    relationships = feature_store["entity_sets"][entity_set_name]["relationships"]
    for (t, p,c) in relationships:
        if p["name"] == target_name and c["name"] == child_name:
            return c["index"]


def get_join_key(entity_set_name, target_name, peer_name):
    relationships = feature_store["entity_sets"][entity_set_name]["relationships"]
    for (t, p, c) in relationships:
        if p["name"] == target_name and c["name"] == peer_name:
            return c["index"]

def get_primary_entities(features):

    for k,v in features["features"].items():
        if feature_store["entities"][k]["type"] == "primary":
            yield feature_store["entities"][k], v


def get_agg_features(features):
    for k,v in features["features"].items():
        entity = feature_store["entities"][k]
        if entity["type"] == "event":
            agg_features = []
            for f in v:
                if f in entity["aggregation_features"].keys():
                    agg_features.append(entity["aggregation_features"][f])

            yield entity, agg_features



def get_raw_features_from_event_table(features):

    entity_set_name = features["entity_set"]
    target = features["target_entity"]
    entity = feature_store["entities"][target]
    if entity["type"] == "event":
        raw = [f for f in entity["raw_features"] if f in features["features"][target]]
        return entity, raw


def get_training_df_el(features):

    entity_set_name = features["entity_set"]
    target = features["target_entity"]
    obs = features["observations"]

    eol = obs["eol"]
    eol["table"] = f"{entity_set_name}_eol"
    if obs["data"] is not None:
        write_eol(entity_set_name, obs["data"])

    eol_tn = eol["table"]
    eol_pk = eol["pk"]
    eol_lb = eol["label"]

    sql1 = f"select {eol_tn}.{eol_pk}, {eol_tn}.{eol_lb}"

    sql2 = f" from {eol_tn}"

    for e, e_fe in get_primary_entities(features):
        e_tn = e["table"]
        e_pk = e["index"]


        for feature in e_fe:
            sql1 += f", {e_tn}.{feature}"

        sql2 += f" left join {e_tn} on {eol_tn}.{eol_pk} = {e_tn}.{e_pk}"



    for child, cf in get_agg_features(features):

        c_tn = child["table"]
        c_parentk = get_parent_key(entity_set_name, target, child["name"])

        for f in cf:
            c_nm = f["name"]
            sql2 += f" left join (select {c_tn}.{c_parentk}, {get_agg_function(c_tn, f)} as {c_nm} from {c_tn} group by {c_tn}.{c_parentk}) {c_tn}{c_nm} on {e_tn}.{e_pk} = {c_tn}{c_nm}.{c_parentk}"
            sql1 += f", {c_tn}{c_nm}.{c_nm}"


    sql = sql1 + sql2

    df = pd.read_sql(sql, connection)

    return df



#this needs a lot of work
def get_event_training_df(features):

    entity_set_name = features["entity_set"]
    target = features["target_entity"]
    obs = features["observations"]

    eol = obs["eol"]
    eol["table"] = f"{entity_set_name}_eol"
    if obs["data"] is not None:
        write_eol(entity_set_name, obs["data"])

    eol_tn = eol["table"]
    eol_pk = eol["pk"]
    eol_od = eol["observation_date"]
    eol_lb = eol["label"]

    sql1 = f"select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {eol_tn}.{eol_lb}"

    sql2 = ""

    sql3_ = f" from {eol_tn} "

    sql3 = ""

    sql4 = ""

    for e, e_fe in get_primary_entities(features):
        e_tn = e["table"]
        e_pk = e["index"]
        e_ed = e["time"]["field"]

        for feature in e_fe:
            sql2 += f", {e_tn}1.{feature}"

        sql3 += f" left join {e_tn} {e_tn}1 on {eol_tn}.{eol_pk} = {e_tn}1.{e_pk} and \
          {e_tn}1.{e_ed} = (select max({e_ed}) from {e_tn} {e_tn}2 where {e_tn}2.{e_pk} = {eol_tn}.{eol_pk} and {e_tn}2.{e_ed} <= {eol_tn}.{eol_od})"


    e, e_fe = get_raw_features_from_event_table(features)
    e_tn = e["table"]
    e_pk = e["index"]
    e_ed = e["time"]["field"]

    for feature in e_fe:
        sql2 += f", {e_tn}.{feature}"

    sql3 += f" left join {e_tn} on {eol_tn}.{eol_pk} = {e_tn}.{e_pk} and \
            {e_tn}.{e_ed} = {eol_tn}.{eol_od}"


    #this only works for the target event...if we want to include other aggregations from other event tables, need to join via some other key.

    for child, cf in get_agg_features(features):

        c_tn = child["table"]
        c_pk = child["index"]
        c_d = child["time"]["field"]

        for f in cf:
            c_nm = f["name"]

            sql4 += f" left join (select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {get_agg_function(c_tn, f)} as {c_nm} from \
                  {eol_tn} join {c_tn} on {eol_tn}.{eol_pk} = {c_tn}.{c_pk} and {eol_tn}.{eol_od} >= {c_tn}.{c_d} group by \
                  {eol_tn}.{eol_pk}, {eol_tn}.{eol_od} ) {c_tn}{c_nm}1 on \
                  {eol_tn}.{eol_pk} = {c_tn}{c_nm}1.{eol_pk} and {eol_tn}.{eol_od} = {c_tn}{c_nm}1.{eol_od}"

            sql2 += f" , {c_tn}{c_nm}1.{c_nm}"

    sql = sql1 + sql2 + sql3_ + sql3 + sql4

    df = pd.read_sql(sql, connection)

    return df


def get_training_df(features):

    entity_set_name = features["entity_set"]
    target = features["target_entity"]
    obs = features["observations"]



    eol = obs["eol"]
    eol["table"] = f"{entity_set_name}_eol"
    if obs["data"] is not None:
        write_eol(entity_set_name, obs["data"])


    eol_tn = eol["table"]
    eol_pk = eol["pk"]
    eol_od = eol["observation_date"]
    eol_lb = eol["label"]

    sql1 = f"select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {eol_tn}.{eol_lb}"

    sql2 = ""

    sql3_ = f" from {eol_tn} "

    sql3 = ""

    sql4 = ""


    #need to get the right join key for non targetr primary_entities...join is through target table not eol_table...meh..could let slide actually..not an unresonable assumption
    for e, e_fe in get_primary_entities(features):
        e_tn = e["table"]
        e_pk = e["index"]
        e_ed = e["time"]["field"]


        for feature in e_fe:
            sql2 += f", {e_tn}1.{feature}"

        sql3 += f" left join {e_tn} {e_tn}1 on {eol_tn}.{eol_pk} = {e_tn}1.{e_pk} and \
          {e_tn}1.{e_ed} = (select max({e_ed}) from {e_tn} {e_tn}2 where {e_tn}2.{e_pk} = {eol_tn}.{eol_pk} and {e_tn}2.{e_ed} <= {eol_tn}.{eol_od})"



    for child, cf in get_agg_features(features):

        c_tn = child["table"]
        c_parentk = get_parent_key(entity_set_name, target, child["name"])
        c_d = child["time"]["field"]

        for f in cf:
            c_nm = f["name"]

            sql4 += f" left join (select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {get_agg_function(c_tn, f)} as {c_nm} from \
                  {eol_tn} join {c_tn} on {eol_tn}.{eol_pk} = {c_tn}.{c_parentk} and {eol_tn}.{eol_od} >= {c_tn}.{c_d} group by \
                  {eol_tn}.{eol_pk}, {eol_tn}.{eol_od} ) {c_tn}{c_nm}1 on \
                  {eol_tn}.{eol_pk} = {c_tn}{c_nm}1.{eol_pk} and {eol_tn}.{eol_od} = {c_tn}{c_nm}1.{eol_od}"

            sql2 += f" , {c_tn}{c_nm}1.{c_nm}"

    sql = sql1 + sql2 + sql3_ + sql3 + sql4

    df = pd.read_sql(sql, connection)

    return df




def get_prediction_df(features):

    entity_set_name = features["entity_set"]
    target = features["target_entity"]
    e = feature_store["entities"][target]

    me_fe = features["features"][target]
    me_tn = e["table"]
    me_pk = e["index"]
    me_ed = e["time"]["field"]

    sql1=f"select {me_tn}.{me_pk}"

    for feature in me_fe:
        sql1 += f", {me_tn}.{feature}"

    sql2 = f" from {me_tn}"

    sql5 = f" where {me_tn}.{me_ed} = (select max({me_ed}) from {me_tn} {me_tn}1 where {me_tn}1.{me_pk} = {me_tn}.{me_pk})"

    sql3 = ""
    sql4 = ""

    for e, e_fe in get_primary_entities(features):
        if e["name"] == target:
            continue

        e_tn = e["table"]
        e_pk = e["index"]
        e_ed = e["time"]["field"]


        for feature in e_fe:
            sql1 += f", {e_tn}.{feature}"

        sql3 = f" left join {e_tn} on {e_tn}.{e_pk} = {me_tn}.{me_pk}"


        sql5 += f" and {e_tn}.{e_ed} = (select max({e_ed}) from {e_tn} {e_tn}1 where {e_tn}1.{e_pk} = {e_tn}.{e_pk})"


    for child, cf in get_agg_features(features):

        c_tn = child["table"]
        c_parentk = get_parent_key(entity_set_name, target, child["name"])
        c_d = child["time"]["field"]

        for f in cf:
            c_nm = f["name"]
            sql4 += f" left join (select {c_tn}.{c_parentk}, {get_agg_function(c_tn, f)} as {c_nm} from {c_tn} group by {c_tn}.{c_parentk}) {c_tn}{c_nm} on {me_tn}.{me_pk} = {c_tn}{c_nm}.{c_parentk}"
            sql1 += f", {c_tn}{c_nm}.{c_nm}"



    sql = sql1 + sql2 + sql3 + sql4 + sql5

    df = pd.read_sql(sql, connection)

    return df






def list_features(feature_set):

    features = {}

    for _, v in feature_store["entities"].items():
        if v["name"] not in feature_store["entity_sets"][feature_set]["entities"]:
            continue
        features[v["name"]] = {}
        features[v["name"]]["raw_features"] = []
        features[v["name"]]["raw_features"].extend(v["raw_features"])

        if "aggregation_features" in v.keys():
            features[v["name"]]["calulated_features"] = []
            features[v["name"]]["calulated_features"].extend(v["aggregation_features"])


    return features




type_mappings = {dtype('int64'): 'numerical', dtype('float64'): 'numerical', dtype('object'): 'category', dtype('bool'): 'binary', dtype('datetime64[ns]'): 'numerical'}


def get_model_definition(features):

    df = get_training_df_el(features)
    target = features["observations"]["eol"]["label"]
    input_features = []

    for col in df.columns:
        if col == target:
            continue
        n = str(df[col].name)
        t = df[col].dtype
        input_features.append({'name': n, 'type': type_mappings[t]})

    model_def = {}
    model_def['input_features'] = input_features
    model_def['output_features'] = [{'name': target, 'type': 'binary'}]  #don't remember why i need to cheat here....

    return model_def

def generate_base_model_definition(features):
    model_def = get_model_definition(features)
    #some versioning would be good
    with open('model_definition.yaml', 'w') as yaml_file:
        yaml.dump(model_def, yaml_file, default_flow_style=False)




