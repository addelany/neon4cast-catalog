generate_model_items <- function(){


  model_list <- aquatic_models$model.id

  x <- purrr::map(model_list, function(i)
    list(
      "rel" = 'item',
      'type'= 'application/json',
      #'href' = paste0("https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/forecasts/models/",i,'.json'))
      'href' = paste0('models/',i,'.json'))
  )

  return(x)
}

build_forecast <- function(table_schema, table_description){
  forecast <- list(
    "id" = "aquatics-forecasts",
    "description" = "pending",
    "stac_version"= "1.0.0",
    "license"= "CC0-1.0",
    "stac_extensions"= list("https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                            "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
                            "https://stac-extensions.github.io/table/v1.2.0/schema.json"),
    'type' = 'Collection',
    'links' = generate_model_items(),
    list(
    "rel" = "parent",
    "type"= "application/json",
    #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/aquatics.json"
    "href" = '../aquatics.json'
    ),
    list(
      "rel" = "root",
      "type" = "application/json",
      #"href" = "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/aquatics.json"
      "href" = '../aquatics.json'
    ),
    list(
      "rel" = "self",
      "type" = "application/json",
      #"href" = "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/forecasts/forecast.json"
      "href" = 'forecast.json'
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
          "2020-09-01T00:00:00Z",
          "2023-04-11T00:00:00Z")
      ))
    ),
    "table_columns" = stac4cast::build_table_columns(table_schema, table_description)
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


## CREATE table for column descriptions
description_create <- data.frame(datetime = 'ISO 8601(ISO 2019)datetime the forecast starts from (a.k.a. issue time); Only needed if more than one reference_datetime is stored in asingle file. Forecast lead time is thus datetime-reference_datetime. Ina hindcast the reference_datetimewill be earlierthan the time thehindcast was actually produced (seepubDatein Section3). Datetimesare allowed to be earlier than thereference_datetimeif areanalysis/reforecast is run before the start of the forecast period. Thisvariable was calledstart_timebefore v0.5 of theEFI standard.',
                                 site_id = 'For forecasts that are not on a spatial grid, use of a site dimension thatmaps to a more detailed geometry (points, polygons, etc.) is allowable.In general this would be documented in the external metadata (e.g., alook-up table that provides lon and lat); however in netCDF this couldbe handled by the CF Discrete Sampling Geometry data model.',
                                 family = 'For ensembles: “ensemble.” Default value if unspecifiedFor probability distributions: Name of the statistical distributionassociated with the reported statistics. The “sample” distribution issynonymous with “ensemble.”For summary statistics: “summary.”If this dimension does not vary, it is permissible to specifyfamilyas avariable attribute if the file format being used supports this (e.g.,netCDF).',
                                 parameter = 'ensemble member',
                                 variable = 'aquatic forecast variable',
                                 prediction = 'predicted forecast value',
                                 pubDate = 'date of publication',
                                 date = 'ISO 8601(ISO 2019)datetime being predicted; follows CF conventionhttp://cfconventions.org/cf-conventions/cf-conventions.html#time-coordinate. This variable was called time before v0.5of the EFIconvention.For time-integrated variables (e.g., cumulative net primary productivity), one should specify thestart_datetimeandend_datetimeas two variables, instead of the singledatetime.If this is not providedthedatetimeis assumed to be the MIDPOINT of theintegrationperiod.')

## just read in example forecast to extract schema information -- ask about better ways of doing this
theme <- 'aquatics'
reference_datetime <- '2023-05-01'
site_id <- 'BARC'
model_id <- 'flareGLM'
variable_name <- 'temperature'

s3_schema <- arrow::s3_bucket(
  bucket = glue::glue("neon4cast-forecasts/parquet/{theme}/",
                      "model_id={model_id}/",
                      "reference_datetime={reference_datetime}/"),
  endpoint_override = "data.ecoforecast.org",
  anonymous = TRUE)
theme_df <- arrow::open_dataset(s3_schema) %>%
  filter(variable == variable_name, site_id == site_id)

## identify model ids from bucket
s3 <- s3_bucket("neon4cast-inventory", endpoint_override="data.ecoforecast.org")
paths <- open_dataset(s3$path("neon4cast-forecasts")) |> collect()
models_df <- paths |> filter(...1 == "parquet", ...2 == "aquatics") |> distinct(...3)
aquatic_models <- models_df |>
  tidyr::separate(...3, c('name','model.id'), "=")


build_forecast(theme_df, description_create)

