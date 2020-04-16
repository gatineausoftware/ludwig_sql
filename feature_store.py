
import sqlalchemy as db
import pandas as pd



engine = db.create_engine('mysql+pymysql://root:swamp@localhost:3306/ml')

connection = engine.connect()


feature_store = {}


def add_entity_set(entity_set):
    feature_store[entity_set] = {"entities": {}, "child_entities": {}}

def add_entity(entity_set, name, table, pk, effective_date, features):

    entity = {"name": name, "table": table, "pk": pk, "effective_date": effective_date, "features": features}
    feature_store[entity_set]["entities"][name] = entity



def add_entity_df(df, entity_set, name, entity_id, effective_date):
    try:

        frame = df.to_sql(name, connection, if_exists='replace');

    except ValueError as vx:
        print(vx)
        return

    except Exception as ex:
        print(ex)
        return



    features = [c for c in df.columns if c not in [entity_id, effective_date]]

    if entity_set not in feature_store.keys():
        add_entity_set(entity_set)

    add_entity(entity_set, name, name, entity_id, effective_date, features)




def add_child_df(df, entity_set, name, entity_id, parent_id, date):
    try:
        frame = df.to_sql(name, connection, if_exists='replace');

    except ValueError as vx:
        print(vx)
        return

    except Exception as ex:
        print(ex)
        return

    if entity_set not in feature_store.keys():
        add_entity_set(entity_set)

    entity_desc = {"name": name, "table": name, "pk": entity_id, "parent_key": parent_id, "date": date, "features": []}

    feature_store[entity_set]["child_entities"][name] = entity_desc




def add_child_features(entity_set, entity, features):

    feature_store[entity_set]["child_entities"][entity]["features"].extend(features)



def write_eol(entity_set, df):
    try:

        frame = df.to_sql(entity_set + "_eol", connection, if_exists='replace');

    except ValueError as vx:
        print(vx)
        return

    except Exception as ex:
        print(ex)
        return





def add_aggregation_child(entity_set, name, table, pk, parent_key, tdate, column, type):
    entity = {"name": name, "table": table, "pk": pk, "parent_key": parent_key, "date": tdate, "column": column, "type": type}
    feature_store[entity_set]["child"].append(entity)


def add_eol(entity_set, table, pk, observation_date, label):
    entity = {"table": table, "pk": pk, "observation_date": observation_date, "label": label}
    feature_store[entity_set]["eol"] = entity



def get_agg_function(table, child_feature):

    #table = child_feature["table"]
    column = child_feature["feature"]
    agg = child_feature["function"]

    if agg=="count":
        return f"count(distinct {table}.{column})"
    elif agg == "max":
        return f"max({table}.{column})"
    elif agg == "sum":
        return f"sum({table}.{column})"



def get_prediction_data_set(entity_set):

    entity_set = feature_store[entity_set]

    sql2 = None
    sql3= None

    for k, e in entity_set["entities"].items():

        e_tn = e["table"]
        e_pk = e["pk"]
        e_ed = e["effective_date"]
        e_fe = e["features"]

        if not sql2:
            sql2=f"select {e_tn}.{e_pk}"
        else:
            pass

        for feature in e_fe:
            sql2 += f", {e_tn}.{feature}"

        if not sql3:
            sql3 = f" from {e_tn}"
        else:
            sql3 += f". {e_tn}"

        break #fix

    sql4=""

    for _, child in entity_set["child_entities"].items():

        c_tn = child["table"]
        c_parentk = child["parent_key"]
        c_d = child["date"]
        c_pk = child["pk"]

        for f in child["features"]:
            c_nm = f["name"]


            sql4 += f" left join (select {c_tn}.{c_parentk}, {get_agg_function(c_tn, f)} as {c_nm} from {c_tn} group by {c_tn}.{c_parentk}) {c_tn}{c_nm} on {e_tn}.{e_pk} = {c_tn}{c_nm}.{c_parentk}"

            sql2 += f", {c_tn}{c_nm}.{c_nm}"

    sql5 = f" where {e_tn}.{e_ed} = (select max({e_ed}) from {e_tn} {e_tn}1 where {e_tn}1.{e_pk} = {e_tn}.{e_pk})"

    sql = sql2 + sql3 + sql4 + sql5

    df = pd.read_sql(sql, connection)

    return df



def get_training_data_set(entity_set, eol, df=None):

    if df is not None:
        write_eol(entity_set, df)
        eol["table"] = f"{entity_set}_eol"


    entity_set = feature_store[entity_set]


    eol_tn = eol["table"]
    eol_pk = eol["pk"]
    eol_od = eol["observation_date"]
    eol_lb = eol["label"]


    sql1 = f"select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {eol_tn}.{eol_lb}"

    sql2 = ""

    sql3_ = f" from {eol_tn} "

    sql3 = ""

    for k, e in entity_set["entities"].items():
        e_tn = e["table"]
        e_pk = e["pk"]
        e_ed = e["effective_date"]
        e_fe = e["features"]



        for feature in e_fe:
            sql2 += f", {e_tn}1.{feature}"



        sql3 += f" left join {e_tn} {e_tn}1 on {eol_tn}.{eol_pk} = {e_tn}1.{e_pk} and \
        {e_tn}1.{e_ed} = (select max({e_ed}) from {e_tn} {e_tn}2 where {e_tn}2.{e_pk} = {eol_tn}.{eol_pk} and {e_tn}2.{e_ed} <= {eol_tn}.{eol_od})"



    sql4 = ""

    for _, child in entity_set["child_entities"].items():

        c_tn = child["table"]
        c_parentk = child["parent_key"]
        c_d = child["date"]


        for f in child["features"]:
            c_nm = f["name"]

            sql4 += f" left join (select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {get_agg_function(c_tn, f)} as {c_nm} from \
                {eol_tn} join {c_tn} on {eol_tn}.{eol_pk} = {c_tn}.{c_parentk} and {eol_tn}.{eol_od} >= {c_tn}.{c_d} group by \
                {eol_tn}.{eol_pk}, {eol_tn}.{eol_od} ) {c_tn}{c_nm}1 on \
                {eol_tn}.{eol_pk} = {c_tn}{c_nm}1.{eol_pk} and {eol_tn}.{eol_od} = {c_tn}{c_nm}1.{eol_od}"


            sql2 += f" , {c_tn}{c_nm}1.{c_nm}"

    sql = sql1 + sql2 + sql3_ + sql3 + sql4

    df = pd.read_sql(sql, connection)

    return df


def get_features(entity_set):

    es = feature_store[entity_set]

    features=[]
    for entity in es["entities"]:
        en = entity["table"]
        for feature in entity["features"]:
            features.append(f"{en}.{feature}")


    for entity in es["child"]:
        en = entity["table"]
        f = entity["name"]
        features.append(f"{en}.{f}")


    return features








