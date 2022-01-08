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
      title = "Connection", 
      selectInput(
        ns("input_conn_id"), 
        "connection", 
        choices = choices_connection_id
          )
    ),
    
    box(
      width = 12, 
      dygraphOutput(ns('global_plot'))
    ),
    
    box(
      width = 12,
      DTOutput(ns('tbl_models'))
    ),
    # mod_new_train_ui("x"),
    box(
      width = 12,
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
    
    df_models_per_connection <- reactive({
      dbFileQuery(
        conn = con, 
        with_query_path("select_template/whale_models_per_connection_id.sql"),
        params = list(connection_id = input$input_conn_id)
      )
    })
    
    selected_model <- reactive({
      req(input$tbl_models_rows_selected)
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
        params = list(connection_id = input$input_conn_id)
      )
    })
    
    df_backcast <- reactive({
      backcast_id <- df_backcast_per_model()$id[input$tbl_backcast_rows_selected]
      read_csv(with_whale_backcast_path(glue("{backcast_id}.csv")))
    })
    
    output$global_plot <- renderDygraph({
      full_join(df_actuals(), df_backcast(), by = "datetime") %>% 
        df_to_ts() %>% 
        dygraph() %>% 
        dyRangeSelector()
    })
    
    ###
      
     
    
    output$tbl_models = renderDT(
      df_models_per_connection(),
      selection = 'single',
      class = "row-border",
      rownames = FALSE,
      options = list(
        scrollX = TRUE,
        conditionalPaging = TRUE,
        pageLength = 10,
        dom = "tip")
    )
    
    output$tbl_backcast = renderDT(
      df_backcast_per_model(),
      selection = 'single'
    )
    
    output$x4 = renderPrint({
      x = input$tbl_models_rows_selected
      if (length(x)) {
        cat(
          df_models_per_connection()$id[x]
        )
      }
    })
    
    # mod_new_server_ui("x")

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
    verbatimTextOutput(ns('x4'))
  )
}

#'  mod_ingest Server Function
#'
#' @noRd 
mod_new_train_server <- function(id, react_df) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    
     
    
    output$x4 = renderPrint({
      cat(range(react_df()$datetime))
    })
    
    
  })
}



