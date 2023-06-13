build_aquatics <- function(start_date,end_date){

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
    "title"= "Ecological Forecasting Initiative - Aquatics",
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
          paste0(start_date,'T00:00:00Z'),
          paste0(end_date,'T00:00:00Z'))
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
    "description" = "This page contains raw forecasts and forecast scores, which are summarized forecasts with evaluation metrics",
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

## READ S3 INVENTORY FOR DATES
s3_inventory <- arrow::s3_bucket("neon4cast-inventory",
                                 endpoint_override = "data.ecoforecast.org",
                                 anonymous = TRUE)

s3_df <- get_grouping(s3_inventory, "aquatics")

s3_df <- s3_df |> filter(model_id != 'null')

theme_max_date <- max(s3_df$date)
theme_min_date <- min(s3_df$date)


build_aquatics(theme_max_date, theme_min_date)
