build_model <- function(model_id, team_name, model_description, first_name, last_name, email, start_date, end_date) {


  meta <- list(
  "stac_version"= "1.0.0",
  "stac_extensions"= list(),
  "type"= "Feature",
  "id"= model_id,
  "bbox"=
        list(-156.6194, 17.9696, -66.7987,  71.2824),
  "geometry"= list(
    "type"= "Polygon",
    "coordinates"= list(
      list(
        list(-156.6194, 17.9696),
        list(-66.7987, 17.9696),
        list(-66.7987, 71.2824),
        list(-156.6194, 71.2824),
        list(-156.6194, 17.9696)
      )
    )
  ),
  "properties"= list(
    #'description' = model_description,
    "description" = glue::glue('# example title

_author names in italics_

Further text with value
'),
    "start_datetime" = start_date,
    "end_datetme" = end_date,
    "providers"= list(
      list(
        "url"= email,
        "name"= paste(first_name,last_name),
        "roles"= list(
          "producer",
          "processor",
          "licensor"
        )
      ),
      list(
        "url"= "https://ecoforecast.org",
        "name"= "Ecoforecast Challenge",
        "roles"= list(
          "host"
        )
      )
    ),
    "license"= "CC0-1.0",
    "keywords"= list(
      "Forecasting",
      "Temperature",
      "Oxygen",
      "NEON")
  ),
  "collection"= "forecast",
  "links"= list(
    list(
      "rel"= "collection",
      'href' = '../collection.json',
      "type"= "application/json",
      "title"= "Aquatics Forecasts"
    ),
    list(
      "rel"= "root",
      'href' = '../../../catalog.json',
      "type"= "application/json",
      "title"= "EFI Forecast Catalog"
    ),
    list(
      "rel"= "parent",
      'href' = '../collection.json',
      "type"= "application/json",
      "title"= "Aquatics Forecasts"
    ),
    list(
      "rel"= "self",
      "href" = paste0(model_id,'.json'),
      "type"= "application/json",
      "title"= "Model Forecast"
    )),
  "assets"= list(
    "parquet_items"= list(
      "href"= paste0("s3://anonymous@",
                     "bio230014-bucket01/neon4cast-forecasts/parquet/",
                     "aquatics/model_id=", model_id,
                     "?endpoint_override=sdsc.osn.xsede.org"),
      "type"= "application/x-parquet",
      "title"= team_name,
      "description"= readr::read_file("stac/aquatics/forecasts/models/asset-description.Rmd")
    )
  )
)


  dest <- "stac/aquatics/forecasts/models/"
  json <- file.path(dest, paste0(model_id, ".json"))

  jsonlite::write_json(meta,
                       json,
                       pretty=TRUE,
                       auto_unbox=TRUE)
  stac4cast::stac_validate(json)


}

get_grouping <- function(s3_inv,
                         theme,
                         collapse=TRUE,
                         endpoint="data.ecoforecast.org") {

  groups <- arrow::open_dataset(s3_inv$path("neon4cast-forecasts")) |>
    dplyr::filter(...1 == "parquet", ...2 == {theme}) |>
    dplyr::select(model_id = ...3, reference_datetime = ...4, date = ...5) |>
    dplyr::mutate(model_id = gsub("model_id=", "", model_id),
                  reference_datetime =
                    gsub("reference_datetime=", "", reference_datetime),
                  date = gsub("date=", "", date)) |>
    dplyr::collect()

}

## read in model documentation and only grab models for Aquatics theme
library(tidyverse)
library(arrow)
library(stac4cast)
library(reticulate)

#get model ids
s3 <- s3_bucket("neon4cast-inventory", endpoint_override="data.ecoforecast.org")
paths <- open_dataset(s3$path("neon4cast-forecasts")) |> collect()
models_df <- paths |> filter(...1 == "parquet", ...2 == "aquatics") |> distinct(...3)

aquatic_models <- models_df |>
  tidyr::separate(...3, c('name','model.id'), "=")


## READ IN MODEL METADATA
model_docs <- read_csv('NEON_Challenge_Registration_2023-05-23.csv')

model_docs <- model_docs |>
  filter(Theme == 'Aquatic Ecosystems') |>
  select(`team-name`, `First Name`, `Last Name`, `Email address`, `model-id`, `model-description`) |>
  rename(team.name = `team-name`, first.name = `First Name`, last.name = `Last Name`, email = `Email address`,
         model.id = `model-id`, model.description = `model-description`) |>
  mutate(model.description = ifelse(is.na(model.description),'',model.description))

## READ S3 INVENTORY FOR DATES
s3_inventory <- arrow::s3_bucket("neon4cast-inventory",
                          endpoint_override = "data.ecoforecast.org",
                          anonymous = TRUE)

s3_df <- get_grouping(s3_inventory, "aquatics")

## loop over model ids and extract components if present in metadata table
for (m in aquatic_models$model.id[1:2]){
  model_date_range <- s3_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  if (m %in% model_docs$model.id){
    print('has metadata')

    idx = which(model_docs$model.id == m)

    build_model(model_id = model_docs$model.id[idx],
                team_name = model_docs$team.name[idx],
                model_description = model_docs[idx,'model.description'][[1]],
                first_name = model_docs$first.name[idx],
                last_name = model_docs$last.name[idx],
                email = model_docs$email[idx],
                model_min_date,
                model_max_date)
  } else{

    build_model(model_id = m,
                team_name = 'pending',
                model_description = 'pending',
                first_name = 'pending',
                last_name = 'pending',
                email = 'pending',
                model_min_date,
                model_max_date)
  }
}
