
source("R/common.R")


catalog <- list(
  "type"= "Catalog",
  "id"= "efi-stac",
  "title"= "Ecological Forecasting Initiative STAC API",
  "description"= "Searchable spatiotemporal metadata describing forecasts by the Ecological Forecasting Initiative",
  "stac_version"= "1.0.0",
  "conformsTo"= conformsTo,
  "links"= list(
    list(
      "rel"= "self",
      "type"= "application/json",
      "href"= "https=//projects.ecoforecast.org/neon4cast-catalog/stac/v1/catalog.json"
    ),
    list(
      "rel"= "root",
      "type"= "application/json",
      "href"= "https=//projects.ecoforecast.org/neon4cast-catalog/stac/v1/catalog.json"
    ),

    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "NOAA Global Ensemble Forecast System",
      "href"= "https=//projects.ecoforecast.org/neon4cast-catalog/stac/v1/collections/noaa.json"
    ),
    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "Aquatics Forecast Challenge",
      "href"= "https=//projects.ecoforecast.org/neon4cast-catalog/stac/v1/collections/aquatics.json"
    ),
    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "Beetles Forecast Challenge",
      "href"= "https=//projects.ecoforecast.org/neon4cast-catalog/stac/v1/collections/beetles.json"
    ),
    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "Terrestrial Forecast Challenge",
      "href"= "https=//projects.ecoforecast.org/neon4cast-catalog/stac/v1/collections/terrestrial.json"
    ),
    list(
      "rel"= "child",
      "type"= "application/json",
      "title"= "Phenology Forecast Challenge",
      "href"= "https=//projects.ecoforecast.org/neon4cast-catalog/stac/v1/collections/phenology.json"
    )
  )
)

dest <- "../stac/v1/"
jsonlite::write_json(catalog, file.path(dest, "catalog.json"),
                     pretty=TRUE, auto_unbox=TRUE)
stac4cast::stac_validate("catalog.json")
