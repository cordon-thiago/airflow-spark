# mod_backcast UI Function
mod_backcast_ui <- function(id){
  ns <- NS(id)
  
  choices_connection_id <- dbFileQuery(
    conn = con, 
    with_query_path("select/distinct_connection_id_whale.sql")
    )$connection_id
  
  fluidPage(
    h1("Backcast"),
    box(
      width = 12,
      selectizeInput(
        inputId = ns("conn_id"),
        label = "Connection",
        choices = choices_connection_id
      )
    ),
    box(
      width = 12,
      dygraphOutput(ns('global_plot'))
    ),
    box(
      width = 12,
      title = "Trained models",
      box(width = 12, 
        DTOutput(ns('tbl_models')),
        actionButton(ns("refresh_models"), "Refresh", icon("sync"))
        ),
      mod_new_train_ui(ns("train"))
    ),
    box(
      width = 12,
      title = "Backcast results",
      box(width = 12,
        DTOutput(ns('tbl_backcast')),
        actionButton(ns("refresh_backcast"), "Refresh", icon("sync"))
      ),
      mod_new_backcast_ui(ns("backcast"))
    )
  )
}

# mod_backcast Server Function
mod_backcast_server <- function(id, input_list, code) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    # reactive variables
    selected_connection <- reactive(input$conn_id)
    
    selected_model <- reactive({
      req(selected_connection())
      df_models_per_connection()$id[input$tbl_models_rows_selected]
    })
    
    # queried data frames for tables
    df_models_per_connection <- eventReactive(c(selected_connection(), input$refresh_models), {
      dbFileQuery(
        conn = con,
        with_query_path("select_template/whale_models_per_connection_id.sql"),
        params = list(connection_id = selected_connection())
      )
    })
    
    df_backcast_per_model <- eventReactive(c(selected_model(), input$refresh_backcast), {
      dbFileQuery(
        conn = con,
        with_query_path("select_template/whale_backcast_per_model.sql"),
        params = list(model_id = selected_model())
      )
    })

    # dataframes for time series
    df_actuals <- reactive({
      dbFileQuery(
        conn = con,
        with_query_path("select_template/whale_all_consumption_per_connection.sql"),
        params = list(connection_id = selected_connection())
      )
    })

    df_backcast <- reactive({
      backcast_id <- df_backcast_per_model()$id[input$tbl_backcast_rows_selected]
      csv_path <- with_whale_backcast_path(glue("{backcast_id}.csv"))
      # propagate downstream failure to read the file by returning empty df
      if (!isTRUE(file.exists(csv_path))) {return(data.frame())}
      read_csv(csv_path)
    })

    # outputs
    output$tbl_models <- render_dt_styled({
      reactive(
        df_models_per_connection() %>% 
          select(-connection_id)
      )
      })
    
    output$tbl_backcast  <- render_dt_styled({
      reactive(
        df_backcast_per_model() %>% 
          select(-model_id)
      )
      })
    
    output$global_plot <- renderDygraph({
      df <- df_actuals()
      if (nrow(df_backcast() > 0)){df <- full_join(df, df_backcast(), by = "datetime")}
      
      df_to_ts(df) %>%
        dygraph() %>%
        dyShading(
          from = df_models_per_connection()$date_train_from[input$tbl_models_rows_selected],
          to = df_models_per_connection()$date_train_until[input$tbl_models_rows_selected]
        ) %>% 
        dyRangeSelector()
    })
    
    # modules
    mod_new_train_server(
      "train",
      react_df = df_actuals,
      react_conn_id = reactive(selected_connection())
      )
    
    mod_new_backcast_server(
      "backcast", 
      react_df = df_actuals, 
      react_model_id = reactive(selected_model())
      )
  })
}

# extra modules -----------------------------------------------------------
# new_train
mod_new_train_ui <- function(id){
  ns <- NS(id)
  
  box(
    width = 12,
    title = "New model training",
    collapsible = TRUE,
    collapsed = TRUE,
    uiOutput(ns('info_range')),
    flowLayout(
      dateRangeInput(ns("date_range_trigger"), "Date range:"),
      selectInput(
        ns("method_trigger"), 
        "Method/Algorithm", 
        list(LinearRegression = "lr", RandomForestRegressor = "rf")
        )
    ),
    verbatimTextOutput(ns('config_text')),
    actionButton(ns("trigger_dag"), label = "Send to Airflow", icon = icon("fan"))
  )
}

mod_new_train_server <- function(id, react_df, react_conn_id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    allowed_dttm_range <- reactive({
      range <- range(react_df()$datetime)
      list(
        min = range[1],
        max = range[2]
      )
    })
    
    observeEvent(allowed_dttm_range(), {
      updateDateRangeInput(
        session = session,
        'date_range_trigger',
        start = as_date(allowed_dttm_range()$min),
        end = as_date(allowed_dttm_range()$max),
        min = as_date(allowed_dttm_range()$min),
        max = as_date(allowed_dttm_range()$max)
      )
    })
    
    output$info_range <- renderUI({
      tags$p("Choose between ", tags$code(allowed_dttm_range()$min), 
             " (start timeseries) and ", tags$code(allowed_dttm_range()$max), " (end timeseries)")
    })
    
    airflow_config <- reactive({
      list(
        connection_id = react_conn_id(),
        date_from = date_to_iso8601(input$date_range_trigger[1]),
        date_until = date_to_iso8601(input$date_range_trigger[2]),
        method = input$method_trigger
      )
    })
    
    output$config_text = renderPrint({
      cat(toJSON(airflow_config(), auto_unbox = TRUE, pretty = TRUE))
    })
    
    observeEvent(input$trigger_dag, {
      trigger_dag(
        dag_id = "run_train_one_model",
        params = airflow_config()
      )
    })
  })
}

# new backcast
mod_new_backcast_ui <- function(id){
  ns <- NS(id)
  
  box(
    width = 12,
    title = "New backcast evaluation",
    collapsible = TRUE,
    collapsed = TRUE,
    uiOutput(ns('info_range')),
    flowLayout(
      dateRangeInput(ns("date_range_trigger"), "Date range:")
    ),
    verbatimTextOutput(ns('config_text')),
    actionButton(ns("trigger_dag"), label = "Send to Airflow", icon = icon("fan"))
  )
}

mod_new_backcast_server <- function(id, react_df, react_model_id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    model_properties <- reactive({
      req(react_model_id())
      dbFileQuery(
        conn = con,
        with_query_path("select_template/whale_model_properties.sql"),
        params = list(model_id = react_model_id())
      )
    })
    
    allowed_dttm_range <- reactive({
      list(
        min = model_properties()$date_train_until,
        max = max(react_df()$datetime)
      )
    })
    
    observeEvent(allowed_dttm_range(), {
      updateDateRangeInput(
        session = session,
        'date_range_trigger',
        start = as_date(allowed_dttm_range()$min),
        end = as_date(allowed_dttm_range()$max),
        min = as_date(allowed_dttm_range()$min),
        max = as_date(allowed_dttm_range()$max)
      )
    })
    
    output$info_range <- renderUI({
      tags$p("Choose between ", tags$code(allowed_dttm_range()$min), 
             " (end training model) and ", tags$code(allowed_dttm_range()$max), " (end timeseries)")
    })
    
    airflow_config <- reactive({
      req(react_model_id())
      list(
        model_id = react_model_id(),
        date_from = date_to_iso8601(input$date_range_trigger[1]),
        date_until = date_to_iso8601(input$date_range_trigger[2])
      )
    })

    output$config_text = renderPrint({
      cat(toJSON(airflow_config(), auto_unbox = TRUE, pretty = TRUE))
    })
    
    observeEvent(input$trigger_dag, {
      req(react_model_id())
      trigger_dag(
        dag_id = "run_backcast_one_model",
        params = airflow_config()
      )
    })
  })
}

