build_model <- function(model_id, team_name, model_description, first_name, last_name, email) {


  meta <- list(
  "description" = model_description,
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
    "datetime"= "2023-04-26T00:00:00Z"
  ),
  "collection"= "forecast",
  "links"= list(
    list(
      "rel"= "collection",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/forecasts/forecast.json",
      'href' = '../forecast.json',
      "type"= "application/json",
      "title"= "Aquatics Forecasts"
    ),
    list(
      "rel"= "root",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/catalog.json", #catalog json file
      'href' = '../../../catalog.json',
      "type"= "application/json",
      "title"= "EFI Forecast Catalog"
    ),
    list(
      "rel"= "parent",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/forecasts/forecast.json", #forecast.json
      'href' = '../forecast.json',
      "type"= "application/json",
      "title"= "Aquatics Forecasts"
    ),
    list(
      "rel"= "self",
      #"href"= paste0('https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics/forecasts/models/',model_id,'.json'),
      "href" = paste0('https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/aquatics/forecasts/models/',model_id,'.json'),
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
  ),
   "license"= "CC0-1.0",
  "keywords"= list(
    "Forecasting",
    "Temperature",
    "Oxygen",
    "NEON"
  ),
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


## loop over model ids and extract components if present in metadata table
for (m in aquatic_models$model.id){

  if (m %in% model_docs$model.id){

    idx = which(model_docs$model.id == m)

    build_model(model_id = model_docs$model.id[idx],
                team_name = model_docs$team.name[idx],
                model_description = model_docs[idx,'model.description'],
                first_name = model_docs$first.name[idx],
                last_name = model_docs$last.name[idx],
                email = model_docs$email[idx])
  } else{

    build_model(model_id = m,
                team_name = 'pending',
                model_description = 'pending',
                first_name = 'pending',
                last_name = 'pending',
                email = 'pending')
  }
}
