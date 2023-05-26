
source("R/common.R")

build_catalog <- function(){
catalog <- list(
  "type"= "Catalog",
  "id"= "efi-stac",
  "title"= "Ecological Forecasting Initiative STAC API",
  "description"= "Searchable spatiotemporal metadata describing forecasts by the Ecological Forecasting Initiative",
  "stac_version"= "1.0.0",
  "conformsTo"= 'conformsTo',
  "links"= list(
    list(
      "rel"= "self",
      "type"= "application/json",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/catalog.json"
      "href" = 'https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/catalog.json'
    ),
    list(
      "rel"= "root",
      "type"= "application/json",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/catalog.json"
      "href" = 'https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/catalog.json'
    ),

    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "NOAA Global Ensemble Forecast System",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/noaa.json"
      "href" = 'https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/noaa/noaa.json'
    ),
    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "Aquatics Forecast Challenge",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/aquatics.json"
      "href" = 'https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/aquatics/aquatics.json'

    ),
    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "Beetles Forecast Challenge",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/beetles.json"
      "href" = 'https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/beetles/beetles.json'

      ),
    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "Terrestrial Forecast Challenge",
      #"href"= "https://projects.ecoforecast.org/neon4cast-catalog/stac/terrestrial.json"
      "href" = 'https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/terrestrial/terrestrial.json'

    ),
    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "Phenology Forecast Challenge",
      #"href"= "https=:/projects.ecoforecast.org/neon4cast-catalog/stac/phenology.json"
      "href" = 'https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/pheonology/phenology.json'

    )
  )
)

dest <- "stac/"
jsonlite::write_json(catalog, file.path(dest, "catalog.json"),
                     pretty=TRUE, auto_unbox=TRUE)
stac4cast::stac_validate(file.path(dest, "catalog.json"))

}

build_catalog()
