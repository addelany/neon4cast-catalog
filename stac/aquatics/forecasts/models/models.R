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
    "datetime"= "2023-04-26T00=00=00Z"
  ),
  "collection"= "forecast",
  "links"= list(
    list(
      "rel"= "collection",
      "href"= "../forecast.json",
      "type"= "application/json",
      "title"= "Aquatics Forecasts"
    ),
    list(
      "rel"= "root",
      "href"= "../../../catalog.json",
      "type"= "application/json",
      "title"= "EFI Forecast Catalog"
    ),
    list(
      "rel"= "parent",
      "href"= "../forecast.json",
      "type"= "application/json",
      "title"= "Aquatics Forecasts"
    )
  ),
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
library(stac4cast)
library(reticulate)

model_docs <- read_csv('NEON_Challenge_Registration_2023-05-23.csv')

aquatic_models <- model_docs |>
  filter(Theme == 'Aquatic Ecosystems') |>
  select(`team-name`, `First Name`, `Last Name`, `Email address`, `model-id`, `model-description`) |>
  rename(team.name = `team-name`, first.name = `First Name`, last.name = `Last Name`, email = `Email address`,
         model.id = `model-id`, model.description = `model-description`) |>
  mutate(model.description = ifelse(is.na(model.description),'',model.description))

# loop over model ids and extract components
for (i in seq.int(1,nrow(aquatic_models))){
  #print(aquatic_models[i,'model.description'])

  build_model(model_id = aquatic_models$model.id[i],
              team_name = aquatic_models$team.name[i],
              model_description = aquatic_models[i,'model.description'],
              first_name = aquatic_models$first.name[i],
              last_name = aquatic_models$last.name[i],
              email = aquatic_models$email[i])
}

#build_model("cb_prophet", "")
