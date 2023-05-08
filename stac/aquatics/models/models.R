build_model <- function(model_id, description) {


  meta <- list(
  "description" = "Python-based implementation of Meta's Prophet Model, forecasting aquatics variables using univariate timeseries only.",
  "stac_version"= "1.0.0",
  "stac_extensions"= list(),
  "type"= "Feature",
  "id"= "cb_prophet",
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
  "collection"= "aquatics",
  "links"= list(
    list(
      "rel"= "collection",
      "href"= "../aquatics.json",
      "type"= "application/json",
      "title"= "Aquatics Forecasts"
    ),
    list(
      "rel"= "root",
      "href"= "../../catalog.json",
      "type"= "application/json",
      "title"= "EFI Forecast Catalog"
    ),
    list(
      "rel"= "parent",
      "href"= "../aquatics.json",
      "type"= "application/json",
      "title"= "Aquatics Prophet Model"
    )
  ),
  "assets"= list(
    "parquet_items"= list(
      "href"= paste0("s3://anonymous@",
                     "bio230014-bucket01/neon4cast-forecasts/parquet/",
                     "aquatics/model_id=", model_id,
                     "?endpoint_override=sdsc.osn.xsede.org"),
      "type"= "application/x-parquet",
      "title"= "Trivial Forecasts using Meta's Prophet Model",
      "description"= readr::read_file("stac/aquatics/models/asset-description.Rmd")
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
      "url"= "https=//carlboettiger.info",
      "name"= "Carl Boettiger",
      "roles"= list(
        "producer",
        "processor",
        "licensor"
      )
    ),
    list(
      "url"= "https=//ecoforecast.org",
      "name"= "Ecoforecast Challenge",
      "roles"= list(
        "host"
      )
    )
  )
)


  dest <- "stac/aquatics/models/"
  json <- file.path(dest, paste0(model_id, ".json"))

  jsonlite::write_json(meta,
                       json,
                       pretty=TRUE,
                       auto_unbox=TRUE)
  stac4cast::stac_validate(json)


}

# loop over model ids and extract components
build_model("cb_prophet", "")

