library(shiny)
library(shinydashboard)
library(reticulate)
library(here)

source(here("r_tabs/ingest.R"), local = TRUE)
source(here("r_tabs/backcast.R"), local = TRUE)

ui <- function(request) {
  
  sidebar <- dashboardSidebar(
    sidebarMenu(
      menuItem("Ingest", tabName = "ingest"),
      menuItem("Backcast", tabName = "backcast")
      
    )
  )
  
  body <- dashboardBody(
    tabItems(
      tabItem(tabName = "ingest",
             mod_ingest_ui("x")
      ),
      
      tabItem(tabName = "backcast",
              mod_backcast_ui("x")
      )
    )
  )
  
  dashboardPage(
    dashboardHeader(),
    sidebar, 
    body
  )
}

server <- function(input, output, session) {
  
  mod_ingest_server("x")
  mod_backcast_server("x")
  
  # input_original <- mod_import_data_server("x")
  # input_final <- mod_vars_cont_fcts_server("x", input_list = input_original)
  # 
  # unevaluated_plot_code <- mod_gg_layers_server("x", input_list = input_final)
  # 
  # evaluated_plot <- mod_ace_editor_server(
  #   "x",
  #   input_list = input_final,
  #   code = unevaluated_plot_code
  # )
  # 
  # mod_out_table_server("x", input_list = input_final)
  # mod_out_plot_server("x", plot = evaluated_plot)
  # mod_download_plot_server("x", plot = evaluated_plot)
}

shinyApp(ui = ui, server = server)