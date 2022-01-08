#' mod_backcast UI Function
#'
#' @param id,input,output,session Internal parameters for {shiny}.
#'
#' @noRd 
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
      DTOutput(ns('tbl_models')),
      actionButton(ns("refresh_models"), "Refresh", icon("sync")),
      mod_new_train_ui(ns("xy"))
    ),
    box(
      width = 12,
      title = "Backcast results",
      DTOutput(ns('tbl_backcast'))
    ),
    verbatimTextOutput(ns('x4'))
  )
  
  
}

#'  mod_backcast Server Function
#'
#' @noRd 
mod_backcast_server <- function(id, input_list, code) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    
    df_models_per_connection <- eventReactive(c(input$conn_id, input$refresh_models), {
      dbFileQuery(
        conn = con,
        with_query_path("select_template/whale_models_per_connection_id.sql"),
        params = list(connection_id = input$conn_id)
      )
    })
      

    selected_model <- reactive({
      # req(input$tbl_models_rows_selected)
      df_models_per_connection()$id[input$tbl_models_rows_selected]
    })

    df_backcast_per_model <- reactive({
      dbFileQuery(
        conn = con,
        with_query_path("select_template/whale_backcast_per_model.sql"),
        params = list(model_id = selected_model())
      )
    })

    ###

    df_actuals <- reactive({
      dbFileQuery(
        conn = con,
        with_query_path("select_template/whale_all_consumption_per_connection.sql"),
        params = list(connection_id = input$conn_id)
      )
    })

    df_backcast <- reactive({
      backcast_id <- df_backcast_per_model()$id[input$tbl_backcast_rows_selected]
      read_csv(with_whale_backcast_path(glue("{backcast_id}.csv")))
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

    ###


    output$tbl_models <- render_dt_styled(df_models_per_connection)
    output$tbl_backcast  <- render_dt_styled({
      reactive(
        df_backcast_per_model() %>%
          select(- model_id)
      )
      })
    
    output$x4 = renderPrint({
      cat(
        jsonlite::toJSON(df_models_per_connection())
      )
    })
    
    mod_new_train_server("xy", react_df = df_actuals, react_conn_id = reactive(input$conn_id))

  })
}


# extra modules -----------------------------------------------------------

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
      selectInput(ns("method_trigger"), "Method/Algorithm", c("lr", "rf"))
    ),
    actionButton(ns("trigger_dag"), label = "Send to Airflow", icon = icon("fan")),
    verbatimTextOutput(ns('random_text'))
  )
}

#'  mod_ingest Server Function
#'
#' @noRd 
mod_new_train_server <- function(id, react_df, react_conn_id) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
    output$info_range <- renderUI({
      range <- range(react_df()$datetime)
      tags$p("Choose between ", tags$code(range[1]), 
             " (start timeseries) and ", tags$code(range[2]), " (end timeseries)")
    })
    
    airflow_config <- reactive({
      list(
        connection_id = react_conn_id(),
        date_from = input$date_range_trigger[1],
        date_until = input$date_range_trigger[2],
        method = input$method_trigger
      )
    })
    
  
    output$random_text = renderPrint({
      cat(toJSON(airflow_config(), auto_unbox = TRUE, pretty = TRUE))
    })
    
    observeEvent(input$trigger_dag, {
      trigger_dag(
        dag_id = "train_one_model",
        params = airflow_config()
      )
      showNotification("Job pushed to Airflow")
    })

  })
}



