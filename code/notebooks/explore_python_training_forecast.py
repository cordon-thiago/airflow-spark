import sys
import pandas.io.sql as sqlio
import psycopg2
from pathlib import Path
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor


sys.path.append("/usr/local/modules")
from database_interaction import sql_file_to_df, postgres_connect


sql_path = Path("/usr/local/sql")


import atexit
conn = postgres_connect()

def close_connection():
    if 'conn' in locals(): conn.close()
    
atexit.register(close_connection)
    
    
# params: connection_id, date_train_from, date_train_until, algorithm = (lr, rf)
      
      
dfoo = sql_file_to_df(
    conn = conn, 
    file = sql_path / "select_template" / "whale_ptu_factors_train_consumption.sql",
    params = {
        "connection_id": "871685900000000059MV",
        "date_from": "2018-01-01",
        "date_until": "2022-01-03"
      }
  )
  
  ###
  
dfoo_ready = (
    dfoo
    .sort_values('datetime')
    .assign(temperature = lambda x: x["temperature"].interpolate())
    .assign(solar_radiation = lambda x: x["solar_radiation"].interpolate())
    .assign(is_working_day = lambda x: x["is_working_day"].astype(int))
    # just to not include rows where `temperature` and `solar_radiation` are not interpolated
    .dropna(subset=['temperature', 'solar_radiation'])
    # TODO: what if there are missing values?
    .dropna(subset=['consumption'])
)

###

until_row = int(len(dfoo_ready)*(80/100))

train = dfoo_ready[:until_row]
test = dfoo_ready[until_row:]

# in reality, "train" is all the initial data.

train_features = ['temperature', 'solar_radiation', 'is_working_day', 'sfm_sin', 'sfm_cos', 'doy_sin', 'doy_cos']

c_features_train = train[train_features]

c_labels_train = np.array(train['consumption'])

###

ml_model = LinearRegression().fit(c_features_train, c_labels_train)

###

import uuid
import pickle
import math
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_percentage_error


def mr_save_model(model, id):
    pickle.dump(
        model,
        open(f'/usr/local/remote_data/whale_models/{id}.pkl', 'wb'),
        pickle.HIGHEST_PROTOCOL
        )
    return True
  
def mr_load_model(id):
    return pickle.load(
        open(f'/usr/local/remote_data/whale_models/{id}.pkl', 'rb')
        )
        
###

the_id = uuid.uuid1()

mr_save_model(ml_model, the_id)

# and new row in `whale_models` with
# id, datetime_creation, connection_id, date_train_from, date_train_until

recalled_model = mr_load_model(the_id)

### and here comes the backcasting. load the model and use a similar function as above to get the data.

# params: model_id, date_train_from, date_train_until

c_features_test = test[train_features]
c_labels_test = np.array(test['consumption'])

###

predictions = recalled_model.predict(c_features_test)
actuals = c_labels_test

mse = mean_squared_error(actuals, predictions)
rmse = math.sqrt(mse)
mae = abs(predictions - actuals)
mape = mean_absolute_percentage_error(actuals, predictions)

print('RMSE:', round(rmse, 2))
print('MAE:', round(np.mean(mae), 2))
print('MAPE:', round(mape, 4))

# and save this dataframe with a name in 
pd.DataFrame({
  "datetime": test.datetime, 
  "consumption_predicted": predictions
})

# and new row in `whale_backcast` with
# id, model_id, datetime_creation, date_backcast_from, date_backcast_until, metrics


