
import sqlalchemy as db
import pandas as pd



engine = db.create_engine('mysql+pymysql://root:swamp@localhost:3306/ml')

connection = engine.connect()


feature_store = {}


def add_entity_set(entity_set):
    feature_store[entity_set] = {"child": []}

def add_entity(entity_set, name, table, pk, effective_date, features):

    entity = {"name": name, "table": table, "pk": pk, "effective_date": effective_date, "features": features}
    feature_store[entity_set]["entity"] = entity



def add_entity_df(entity_set, entity_name, df, entity_id, effective_date):
    try:

        frame = df.to_sql(entity_name, connection, if_exists='replace');

    except ValueError as vx:
        print(vx)
        return

    except Exception as ex:
        print(ex)
        return



    features = [c for c in df.columns if c not in [entity_id, effective_date]]

    add_entity_set(entity_set)

    add_entity(entity_set, entity_name, entity_name, entity_id, effective_date, features)



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


def get_agg_function(child):

    table = child["table"]
    column = child["column"]
    agg = child["type"]

    if agg=="count":
        return f"count(distinct {table}.{column})"
    elif agg == "max":
        return f"max({table}.{column})"



def get_prediction_data_set(entity_set):

    e = feature_store[entity_set]["entity"]
    e_tn = e["table"]
    e_pk = e["pk"]
    e_ed = e["effective_date"]
    e_fe = e["features"]

    sql2=f"select {e_tn}.{e_pk}"

    for feature in e_fe:
            sql2 += f", {e_tn}.{feature}"



    sql3 = f" from {e_tn}"

    sql4 = ""

    for child in feature_store[entity_set]["child"]:
        c_nm = child["name"]
        c_tn = child["table"]
        c_pk = child["pk"]
        c_parentk = child["parent_key"]
        c_d = child["date"]

        sql4 += f" left join (select {c_tn}.{c_parentk}, {get_agg_function(child)} as {c_nm} from {c_tn} group by {c_tn}.{c_parentk}) {c_tn}{c_nm} on {e_tn}.{e_pk} = {c_tn}{c_nm}.{c_parentk}"

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


    e= entity_set["entity"]
    e_tn = e["table"]
    e_pk = e["pk"]
    e_ed = e["effective_date"]
    e_fe = e["features"]



    for feature in e_fe:
        sql2 += f", {e_tn}1.{feature}"


    sql3 = f" from {eol_tn} left join {e_tn} {e_tn}1 on {eol_tn}.{eol_pk} = {e_tn}1.{e_pk} and \
    {e_tn}1.{e_ed} = (select max({e_ed}) from {e_tn} {e_tn}2 where {e_tn}2.{e_pk} = {eol_tn}.{eol_pk} and {e_tn}2.{e_ed} <= {eol_tn}.{eol_od})"



    sql4 = ""

    for child in entity_set["child"]:
        c_nm = child["name"]
        c_tn = child["table"]
        c_pk = child["pk"]
        c_parentk = child["parent_key"]
        c_d = child["date"]

        sql4 += f" left join (select {eol_tn}.{eol_pk}, {eol_tn}.{eol_od}, {get_agg_function(child)} as {c_nm} from \
            {eol_tn} join {c_tn} on {eol_tn}.{eol_pk} = {c_tn}.{c_parentk} and {eol_tn}.{eol_od} >= {c_tn}.{c_d} group by \
            {eol_tn}.{eol_pk}, {eol_tn}.{eol_od} ) {c_tn}{c_nm}1 on \
            {eol_tn}.{eol_pk} = {c_tn}{c_nm}1.{eol_pk} and {eol_tn}.{eol_od} = {c_tn}{c_nm}1.{eol_od}"


        sql2 += f" , {c_tn}{c_nm}1.{c_nm}"



    sql = sql1 + sql2 + sql3 + sql4

    df = pd.read_sql(sql, connection)

    return df

