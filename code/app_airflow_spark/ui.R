
sidebar <- dashboardSidebar(
  sidebarMenu(
    menuItem("Ingest", tabName = "ingest", icon = icon("file")),
    menuItem("Trigger", tabName = "trigger", icon = icon("bolt"))
  )
)

body <- dashboardBody(
  tabItems(
    tabItem(tabName = "ingest",
            source(here("r_tabs/ingestor_ui.R"), local = TRUE)$value
    ),
    
    tabItem(tabName = "trigger",
            h2("Widgets tab content")
    )
  )
)



dashboardPage(
  dashboardHeader(),
  sidebar, 
  body
)