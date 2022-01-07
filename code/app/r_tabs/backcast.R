#' mod_backcast UI Function
#'
#' @param id,input,output,session Internal parameters for {shiny}.
#'
#' @noRd 
mod_backcast_ui <- function(id){
  ns <- NS(id)
  
  fluidPage(
    title = "Backcast",
    box(
      width = 12,
      title = "Connection"
    ),
    
    box(
      width = 12,
      title = "Your backcast code here"
    )
  )
  
  
}

#'  mod_backcast Server Function
#'
#' @noRd 
mod_backcast_server <- function(id, input_list, code) {
  moduleServer(id, function(input, output, session) {
    ns <- session$ns
    

  })
}