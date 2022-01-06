import sys
import pandas as pd
import uuid
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
import os
import pickle
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error
import math
import json

sys.path.append("/usr/local/modules")
from database_interaction import sql_file_to_df, postgres_insert_df

def save_model_whale(model, id):
    pickle.dump(
        model,
        open(f'/usr/local/remote_data/whale_models/{id}.pkl', 'wb'),
        pickle.HIGHEST_PROTOCOL
    )
    return True


def load_model_whale(id):
    return pickle.load(
        open(f'/usr/local/remote_data/whale_models/{id}.pkl', 'rb')
    )

def train_model(
        conn,
        connection_id,
        date_from, date_until,
        method="lr",
        model_id=None,
        sql_path="/usr/local/sql"
):
    if model_id is None:
        model_id = uuid.uuid4()

    list_valid_methods = ["lr", "rf"]
    if method not in list_valid_methods:
        raise ValueError(f"valid `method`: {list_valid_methods}")

    features_labels = _whale_consumption_factors_labels(
        conn=conn,
        connection_id=connection_id,
        date_from=date_from,
        date_until=date_until,
        sql_path=sql_path
    )
    
    available_methods ={
        "lr" : LinearRegression, 
        "rf" : RandomForestRegressor
        }
        
    ml_model = available_methods[method]().fit(features_labels["features"], features_labels["labels"])

    # save picked model in repository
    save_model_whale(ml_model, model_id)

    dict_whale_model = {
        "id": model_id,
        "date_train_from": date_from,
        "date_train_until": date_until,
        "connection_id": connection_id,
        "method": method
    }

    # insert row in `whale_models`
    postgres_insert_df(
        conn=conn,
        df=pd.DataFrame(dict_whale_model, index=[0]),
        table="whale_models"
    )
    # return information inserted
    return dict_whale_model


def backcast_model(
    conn,
    model_id,
    date_from, date_until,
    backcast_id=None,
    sql_path="/usr/local/sql"):

    if backcast_id is None:
        backcast_id = uuid.uuid4()

    model_properties = sql_file_to_df(
        conn=conn,
        file=os.path.join(sql_path, "select_template", "whale_model_properties.sql"),
        params={
            "model_id": model_id,
        }
    )

    fetched_connection_id = model_properties["connection_id"][0]
    fetched_date_train_until = model_properties["date_train_until"][0]

    if date_from < fetched_date_train_until:
        raise AssertionError(
            f"model training latest date {fetched_date_train_until} cannot be later than model backcasting earliest {date_from}")

    features_labels = _whale_consumption_factors_labels(
        conn=conn,
        connection_id=fetched_connection_id,
        date_from=date_from,
        date_until=date_until,
        sql_path=sql_path
    )

    fetched_model = load_model_whale(model_id)

    predictions = fetched_model.predict(features_labels["features"])
    actuals = features_labels["labels"]

    mse = mean_squared_error(actuals, predictions)
    rmse = math.sqrt(mse)
    mae = abs(predictions - actuals)
    mape = mean_absolute_percentage_error(actuals, predictions)

    metrics_dict = {
        "rmse": round(rmse, 2),
        "mae": round(np.mean(mae), 2),
        "mape": round(mape, 4)
    }

    # save dataframe with backcast
    df_backcast = pd.DataFrame({
        "datetime": features_labels["datetime"],
        "consumption_predicted": predictions
    })
    df_backcast.to_csv(f'/usr/local/remote_data/whale_backcast/{backcast_id}.csv', index=False)

    # push new entry to `whale_backcast`
    dict_backcast = {
        "id": backcast_id,
        "model_id": model_id,
        "date_backcast_from": date_from,
        "date_backcast_until": date_until,
        "metrics": json.dumps(metrics_dict)
    }

    postgres_insert_df(
        conn=conn,
        df=pd.DataFrame(dict_backcast, index=[0]),
        table="whale_backcast"
    )

    return dict_backcast


def _whale_consumption_factors_labels(conn, connection_id, date_from, date_until, sql_path="/usr/local/sql"):
    df_fetch = sql_file_to_df(
        conn=conn,
        file=os.path.join(sql_path, "select_template", "whale_ptu_factors_train_consumption.sql"),
        params={
            "connection_id": connection_id,
            "date_from": date_from,
            "date_until": date_until
        }
    )

    df_clean = (
        df_fetch
        .sort_values('datetime')
        .assign(temperature=lambda x: x["temperature"].interpolate())
        .assign(solar_radiation=lambda x: x["solar_radiation"].interpolate())
        .assign(is_working_day=lambda x: x["is_working_day"].astype(int))
        # just to not include rows where `temperature` and `solar_radiation` are not interpolated
        .dropna(subset=['temperature', 'solar_radiation'])
        # TODO: what if there are missing values?
        .dropna(subset=['consumption'])

    )

    features = df_clean[
        ['temperature', 'solar_radiation', 'is_working_day', 'sfm_sin', 'sfm_cos', 'doy_sin', 'doy_cos']]
    labels = np.array(df_clean['consumption'])

    return{"features": features, "labels": labels, "datetime": np.array(df_clean['datetime'])}


