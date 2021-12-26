library(shiny)
library(shinydashboard)
library(reticulate)
library(here)

mod <- import_from_path("mymodule", path = "python_ingestors/")
mod$greeting("hello Carlos")

import("pandas")
