library(shiny)
library(shinydashboard)
library(reticulate)
library(here)

mod <- import_from_path("mymodule", path = "py_modules/")
mod$greeting("hello")
