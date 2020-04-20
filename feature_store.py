
import sqlalchemy as db
import pandas as pd



engine = db.create_engine('mysql+pymysql://root:swamp@localhost:3306/ml')

connection = engine.connect()


feature_store = {}


def add_entity_set(entity_set):
    feature_store[entity_set] = {"entities": {}, "child_entities": {}}


def add_entity_table(entity_set, name, table, pk, effective_date, features):

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

    add_entity_table(entity_set, name, name, entity_id, effective_date, features)




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


def add_child_table(entity_set, table, entity_id, parent_id, date):
    if entity_set not in feature_store.keys():
        add_entity_set(entity_set)

    entity_desc = {"name": table, "table": table, "pk": entity_id, "parent_key": parent_id, "date": date, "features": []}

    feature_store[entity_set]["child_entities"][table] = entity_desc


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





def get_agg_function(table, child_feature):

    column = child_feature["feature"]
    agg = child_feature["function"]

    if agg=="count":
        return f"count(distinct {table}.{column})"
    elif agg == "max":
        return f"max({table}.{column})"
    elif agg == "sum":
        return f"sum({table}.{column})"


#need to define one of the tables as the 'master entity' then do left joins from it


def get_prediction_features(entity_set, entity_name, features, entity_rows):
    entity_set = feature_store[entity_set]
    e = entity_set["entities"][entity_name]
    me_tn = e["table"]
    me_pk = e["pk"]
    me_ed = e["effective_date"]
    me_fe = e["features"]

    sql1 = f"select {me_tn}.{me_pk}"

    for feature in me_fe:
        sql1 += f", {me_tn}.{feature}"

    sql2 = f" from {me_tn}"

    sql3 = f" where {me_tn}.{me_ed} = (select max({me_ed}) from {me_tn} {me_tn}1 where {me_tn}1.{me_pk} = {me_tn}.{me_pk})"



    sql3 += f" and {me_tn}.{me_pk} in "


    sql4 = f"({entity_rows[0]}"

    for row in entity_rows[1:]:
        sql4 += f",{row} "

    sql4 += ")"
    sql = sql1 + sql2 + sql3 + sql4


    df = pd.read_sql(sql, connection)

    return df



def get_prediction_data_set(entity_set, root_entity):

    entity_set = feature_store[entity_set]



    e = entity_set["entities"][root_entity]
    me_tn = e["table"]
    me_pk = e["pk"]
    me_ed = e["effective_date"]
    me_fe = e["features"]

    sql1=f"select {me_tn}.{me_pk}"

    for feature in me_fe:
        sql1 += f", {me_tn}.{feature}"

    sql2 = f" from {me_tn}"

    sql5 = f" where {me_tn}.{me_ed} = (select max({me_ed}) from {me_tn} {me_tn}1 where {me_tn}1.{me_pk} = {me_tn}.{me_pk})"

    sql3 = ""
    sql4 = ""



    for k, e in entity_set["entities"].items():
        if k == root_entity:
            continue

        e_tn = e["table"]
        e_pk = e["pk"]
        e_ed = e["effective_date"]
        e_fe = e["features"]

        for feature in e_fe:
            sql1 += f", {e_tn}.{feature}"

        sql3 = f" left join {e_tn} on {e_tn}.{e_pk} = {me_tn}.{me_pk}"


        sql5 += f" and {e_tn}.{e_ed} = (select max({e_ed}) from {e_tn} {e_tn}1 where {e_tn}1.{e_pk} = {e_tn}.{e_pk})"



    for _, child in entity_set["child_entities"].items():

        c_tn = child["table"]
        c_parentk = child["parent_key"]
        c_d = child["date"]
        c_pk = child["pk"]

        for f in child["features"]:
            c_nm = f["name"]


            sql4 += f" left join (select {c_tn}.{c_parentk}, {get_agg_function(c_tn, f)} as {c_nm} from {c_tn} group by {c_tn}.{c_parentk}) {c_tn}{c_nm} on {me_tn}.{me_pk} = {c_tn}{c_nm}.{c_parentk}"

            sql1 += f", {c_tn}{c_nm}.{c_nm}"



    sql = sql1 + sql2 + sql3 + sql4 + sql5

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
    for name, entity in es["entities"].items():
        for feature in entity["features"]:
            features.append(f"{name}.{feature}")

    for name, entity in es["child_entities"].items():

        for feature in entity["features"]:
            fn = feature["name"]
            features.append(f"{name}.{fn}")

    return features








