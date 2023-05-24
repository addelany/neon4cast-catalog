generate_model_items <- function(){

  full_list <- list(
    "rel" = 'item',
    'type'= 'application/json',
    'href' = "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/forecasts/models/"
  )

  for (i in aquatic_models$model.id[1:2]){
    item_list <- list(
      "rel" = 'item',
      'type'= 'application/json',
      'href' = paste0("https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/forecasts/models/",i,'.json'))
    full_list <- append(full_list,list(item_list))
  }

  return(full_list)
}

build_forecast <- function(){
  forecast <- list(
    "id" = "aquatics-forecasts",
    "description" = "pending",
    "stac_version"= "1.0.0",
    "license"= "CC0-1.0",
    "stac_extensions"= list(),
    'type' = 'Collection',
    'links' = list(
      generate_model_items(),
    list(
    "rel" = "parent",
    "type"= "application/json",
    "href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/aquatics.json"
    ),
    list(
      "rel" = "root",
      "type" = "application/json",
      "href" = "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/aquatics.json"
    ),
    list(
      "rel" = "self",
      "type" = "application/json",
      "href" = "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/forecast.json"
    ),
    list(
      "rel" = "cite-as",
      "href" = "https://doi.org/10.1002/fee.2616"
    ),
    list(
      "rel" = "about",
      "href" = "https://projects.ecoforecast.org/neon4cast-catalog/aquatics-catalog.html",
      "type" = "text/html",
      "title" = "Aquatics Forecast Challenge"
    ),
    list(
      "rel" = "describedby",
      "href" = "https://projects.ecoforecast.org/neon4cast-catalog/aquatics-catalog.html",
      "title" = "Organization Landing Page",
      "type" = "text/html"
    )
    ),
    "title" = "Ecological Forecasting Initiative - Aquatics Forecasts",
    "extent" = list(
      "spatial" = list(
        'bbox' = list(list(-149.6106,
                    18.1135,
                    -66.7987,
                    68.6698))
      ),
      "temporal" = list(
        'interval' = list(list(
          "2020-09-01 00:00 Z",
          "2023-04-11 00:00 Z")
      ))
    )
  )


  dest <- "stac/aquatics/forecasts/"
  json <- file.path(dest, "forecast.json")

  jsonlite::write_json(forecast,
                       json,
                       pretty=TRUE,
                       auto_unbox=TRUE)
  stac4cast::stac_validate(json)
}


## create item list based off of aquatic forecast model ids
library(arrow)
library(dplyr)

## identify model ids from bucket
s3 <- s3_bucket("neon4cast-inventory", endpoint_override="data.ecoforecast.org")
paths <- open_dataset(s3$path("neon4cast-forecasts")) |> collect()
models_df <- paths |> filter(...1 == "parquet", ...2 == "aquatics") |> distinct(...3)
aquatic_models <- models_df |>
  tidyr::separate(...3, c('name','model.id'), "=")


build_forecast()

