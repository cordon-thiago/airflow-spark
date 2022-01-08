library(shiny)
library(shinydashboard)
library(reticulate)
library(here)
library(DBI)
library(glue)
library(DT)
library(readr)
library(dygraphs)
library(xts)
library(dplyr)
library(httr)
library(uuid)

# Connect to "jdbc:postgresql://postgres:5432/test"
con <- DBI::dbConnect(
  drv = RPostgres::Postgres(),
  host = "postgres",
  port=5432,
  dbname = "test",
  user = "test",
  password = "postgres"
)

source(here("functions_support.R"), local = TRUE)
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
  
}

shinyApp(ui = ui, server = server)