build_aquatics <- function(){

  aquatics <- list(
    "id" = 'efi-aquatics',
    "type" = "Collection",
    "links" = list(
      list(
        "rel" = "child",
        "type" = "application/json",
        "href" = 'forecasts/collection.json',
        "title" = 'forecast item'
      ),
      list(
        "rel" = "child",
        "type" = "application/json",
        "href" = 'scores/collection.json',
        "title" = 'scores item'
      ),
      list(
        "rel"= "parent",
        "type"= "application/json",
        "href"= "../catalog.json",
        "title" = 'parent'
      ),
      list(
        "rel"= "root",
        "type"= "application/json",
        "href"= "../catalog.json",
        "title" = 'root'
      ),
      list(
        "rel"= "self",
        "type"= "application/json",
        "href" = 'collection.json',
        "title" = 'self'
      ),
      list(
        "rel" ="cite-as",
        "href"= "https://doi.org/10.1002/fee.2616",
        "title" = "citation"
      ),
      list(
        "rel"= "about",
        "href"= "https://projects.ecoforecast.org/neon4cast-catalog/aquatics-catalog.html",
        "type"= "text/html",
        "title"= "Aquatics Forecast Challenge"
      ),
      list(
        "rel"= "describedby",
        "href"= "https://projects.ecoforecast.org/neon4cast-catalog/aquatics-catalog.html",
        "title"= "Organization Landing Page",
        "type"= "text/html"
      )
    ),
    "title"= "Ecological Forecasting Initiative - Aquatics Forecasts",
    'assets' = list(
      'thumbnail' = list(
        "href"= "https://projects.ecoforecast.org/neon4cast-catalog/img/neon_buoy.jpg",
        "type"= "image/JPEG",
        "roles" = list('thumbnail'),
        "title"= "NEON Aquatics Buoy"
      )
    ),
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
    "license" = "CC0-1.0",
    "keywords" = list(
      "Forecasting",
      "Data",
      "Ecology"
    ),
    "providers" = list(
      list(
        "url"= "https://data.ecoforecast.org",
        "name"= "Ecoforecast Data",
        "roles" = list(
          "producer",
          "processor",
          "licensor"
        )
      ),
      list(
        "url"= "https://ecoforecast.org",
        "name"= "Ecoforecast",
        "roles" = list('host')
      )
    ),
    "description" = "The [Ecological Forecasting Initiative](https://ecoforecast.org)",
    "stac_version" = "1.0.0",
    "stac_extensions" = list(
      "https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
      "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
      "https://stac-extensions.github.io/table/v1.2.0/schema.json"
    ),
    "publications" = list(
      "doi" = "https://www.doi.org/10.22541/essoar.167079499.99891914/v1",
      "citation"= "Thomas, R.Q., C. Boettiger, C.C. Carey, M.C. Dietze, L.R. Johnson, M.A. Kenney, J.S. Mclachlan, J.A. Peters, E.R. Sokol, J.F. Weltzin, A. Willson, W.M. Woelmer, and Challenge Contributors. The NEON Ecological Forecasting Challenge. Accepted at Frontiers in Ecology and Environment. Pre-print"
    )
  )


  dest <- "stac/aquatics/"
  json <- file.path(dest, "collection.json")

  jsonlite::write_json(aquatics,
                       json,
                       pretty=TRUE,
                       auto_unbox=TRUE)
  stac4cast::stac_validate(json)
}


build_aquatics()
