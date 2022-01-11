# query -------------------------------------------------------------------
with_query_path <- function(query_file_path){
  file.path("/usr/local/sql", query_file_path)
}

with_whale_backcast_path <- function(query_file_path){
  file.path("/usr/local/remote_data/whale_backcast", query_file_path)
}



dbFileQuery <- function(conn, 
                        file,
                        params = NULL){
  
  if (is.null(params)) {
    return(
      dbGetQuery(
        conn, paste(readLines(file), collapse = '\n')
      )
    )
  }
  
  stopifnot(is.list(params))
  dbGetQuery(
    conn,
    glue_data_sql(
      .x = params,
      paste(readLines(file), collapse = '\n'),
      .open = "{{",
      .close = "}}",
      .con = conn
    )
  )
}


# timeseries --------------------------------------------------------------

df_to_ts <- function(df){
  xts(
    x = df[,2:ncol(df)],
    order.by = df[,1]
  )
}


# UI ----------------------------------------------------------------------

render_dt_styled <- function(react_df){
  
  stopifnot(is.reactive(react_df))
  
  renderDT(
    react_df(),
    selection = 'single',
    class = "row-border",
    rownames = FALSE,
    options = list(
      scrollX = TRUE,
      conditionalPaging = TRUE,
      pageLength = 10,
      dom = "tip")
  )
}


# airflow -----------------------------------------------------------------

trigger_dag <- function(dag_id, params=list(), dag_run_id = UUIDgenerate()){
  url <- glue("http://airflow-webserver:8282/api/v1/dags/{dag_id}/dagRuns")
  POST(url, body = list(conf = params, dag_run_id = dag_run_id), encode = "json")
}

trigger_train_model <- function(connection_id, date_from, date_until, method = "lr"){
  trigger_dag(
    dag_id = "train_one_model",
    params = list(
      connection_id = connection_id,
      date_from = date_from, 
      date_until = date_until,
      method = method
    )
  )
}


trigger_backcast_model <- function(model_id, date_from, date_until){
  trigger_dag(
    dag_id = "backcast_one_model",
    params = list(
      model_id = model_id,
      date_from = date_from, 
      date_until = date_until
    )
  )
}
# trigger_train_model(connection_id = "871685900000000158MV", date_from = "2019-02-22", date_until = "2019-11-11")
# trigger_backcast_model(model_id = "b6d34328-f846-46c0-b51f-e133acd63538", date_from = "2019-11-12", date_until = "2021-06-01")
