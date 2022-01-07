#' mod_ingest UI Function
#'
#' @param id,input,output,session Internal parameters for {shiny}.
#'
#' @noRd 
mod_ingest_ui <- function(id){
  ns <- NS(id)
  
  box(
    width = 12,
    title = "Your ingest code here"
  )
}

#'  mod_ingest Server Function
#'
#' @noRd 
mod_ingest_server <- function(id, input_list, code) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    

  })
}