## MODEL level functions

generate_authors <- function(metadata_available, model_docs){
  if (metadata_available == TRUE){
    f_name_cols <- c('first.name.one','first.name.two','first.name.three','first.name.four','first.name.five','first.name.six','first.name.seven',
                     'first.name.eight','first.name.nine','first.name.ten')
    l_name_cols <- c('last.name.one','last.name.two','last.name.three','last.name.four','last.name.five','last.name.six','last.name.seven',
                     'last.name.eight','last.name.nine','last.name.ten')

    model_first_names <- unlist(model_docs[idx, names(model_docs) %in% f_name_cols], use.names = FALSE)[!is.na(model_docs[idx, names(model_docs) %in% f_name_cols])]
    model_last_names <- unlist(model_docs[idx, names(model_docs) %in% l_name_cols], use.names = FALSE)[!is.na(model_docs[idx, names(model_docs) %in% l_name_cols])]

    x <- purrr::map(seq.int(1:length(model_first_names)), function(i)
      list(
        "url" = 'not provided',
        'name'= paste(model_first_names[i], model_last_names[i]),
        'roles' = list("producer")
      )
    )
    ## SET FIRST AUTHOR INFO
    x[[1]]$url <- unlist(model_docs[idx,'email.one'], use.names = FALSE)
    x[[1]]$roles <- list(
      "producer",
      "processor",
      "licensor"
    )
  } else{
    x <- list(list('url' = 'pending',
                   'name' = 'pending',
                   'roles' = list("producer",
                                  "processor",
                                  "licensor"))
    )
  }
  return(x)
}



build_model <- function(model_id,
                        theme_id,
                        team_name,
                        model_description,
                        start_date,
                        end_date,
                        use_metadata,
                        var_values,
                        var_keys,
                        site_values,
                        model_documentation,
                        destination_path,
                        description_path,
                        aws_download_path,
                        theme_title,
                        collection_name,
                        thumbnail_image_name) {


  preset_keywords <- list("Forecasting", "NEON")
  variables_reformat <- paste(var_values, collapse = ", ")
  site_reformat <- paste(site_values, collapse = ", ")

  aws_asset_link <- paste0("s3://anonymous@bio230014-bucket01/",
                           aws_download_path,
         "/model_id=", model_id,
         "?endpoint_override=sdsc.osn.xsede.org")

  meta <- list(
    "stac_version"= "1.0.0",
    "stac_extensions"= list(),
    "type"= "Feature",
    "id"= model_id,
    "bbox"=
      list(-156.6194, 17.9696, -66.7987,  71.2824),
    "geometry"= list(
      "type"= "MultiPoint",
      "coordinates"= get_site_coords(theme_id, model_id)
    ),
    # "geometry"= list(
    #   "type"= "Polygon",
    #   "coordinates"= list(
    #     list(
    #       list(-156.6194, 17.9696),
    #       list(-66.7987, 17.9696),
    #       list(-66.7987, 71.2824),
    #       list(-156.6194, 71.2824),
    #       list(-156.6194, 17.9696)
    #     )
    #   )
    # ),
    "properties"= list(
      #'description' = model_description,
      "description" = glue::glue('

    model info: {model_description}

    Sites: {site_reformat}

    Variables: {variables_reformat}
'),
      "start_datetime" = start_date,
      "end_datetime" = end_date,
      "providers"= c(generate_authors(metadata_available = use_metadata, model_docs = model_documentation),list(
        list(
          "url"= "https://ecoforecast.org",
          "name"= "Ecoforecast Challenge",
          "roles"= list(
            "host"
          )
        )
      )
      ),
      "license"= "CC0-1.0",
      "keywords"= c(preset_keywords, var_keys)
    ),
    "collection"= collection_name,
    "links"= list(
      list(
        "rel"= "collection",
        'href' = '../collection.json',
        "type"= "application/json",
        "title"= theme_title
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
        "title"= theme_title
      ),
      list(
        "rel"= "self",
        "href" = paste0(model_id,'.json'),
        "type"= "application/json",
        "title"= "Model Forecast"
      )),
    "assets"= c(list(
        "1"= list(
        "href"= aws_asset_link,
        "type"= "application/x-parquet",
        "title"= 'Database Access',
        "description"= readr::read_file(description_path)
      )
      ),
      pull_images('aquatics',model_id,thumbnail_image_name)
    )
  )


  dest <- destination_path
  json <- file.path(dest, paste0(model_id, ".json"))

  jsonlite::write_json(meta,
                       json,
                       pretty=TRUE,
                       auto_unbox=TRUE)
  stac4cast::stac_validate(json)

  rm(meta)


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


generate_vars_sites <- function(m_id, theme){

  # if (m_id %in%  c('GLEON_JRabaey_temp_physics','GLEON_lm_lag_1day','GLEON_physics','USGSHABs1','air2waterSat_2','fARIMA')){
  #   output_info <- c('pending','pending')
  # } else{

  # do this for each theme / model
  info_df <- arrow::open_dataset(info_extract$path(glue::glue("{theme}/model_id={m_id}/"))) |>
    #filter(reference_datetime == "2023-06-18")|> #just grab one EM to limit processing
    collect()

  vars_vector <- sort(unique(info_df$variable))
  sites_vector <- sort(unique(info_df$site_id))

  vars_list <- as.list(sort(unique(info_df$variable)))
  sites_list <- as.list(sort(unique(info_df$site_id)))

  # output_vectors <- c(paste(vars_vector, collapse = ', '),
  #                  paste(sites_vector, collapse = ', '))

  output_list <- list(vars_list,sites_list)

  full_object <- list(vars_vector, sites_vector, output_list)

  return(full_object)
}


## FORECAST LEVEL FUNCTIONS
generate_model_items <- function(){


  model_list <- aquatic_models$model.id

  x <- purrr::map(model_list, function(i)
    list(
      "rel" = 'item',
      'type'= 'application/json',
      'href' = paste0('models/',i,'.json'))
  )

  return(x)
}

pull_images <- function(theme, m_id, image_name){

  info_df <- arrow::open_dataset(info_extract$path(glue::glue("{theme}/model_id={m_id}/"))) |>
    collect()

  sites_vector <- sort(unique(info_df$site_id))

  base_path <- 'https://data.ecoforecast.org/neon4cast-catalog'

  image_assets <- purrr::map(sites_vector, function(i)
    list(
      "href"= file.path(base_path,theme,m_id,i,image_name),
      "type"= "image/png",
      "title"= paste0('Latest Results for ', i),
      "description"= 'Image from s3 storage',
      "roles" = list('thumbnail')
    )
  )

  return(image_assets)

}

get_site_coords <- function(theme, m_id){

  theme_select <- glue::glue('{theme}')

  theme_sites <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv", col_types = cols()) |>
    dplyr::filter(UQ(sym(theme_select)) == 1)

  model_sites <- arrow::open_dataset(info_extract$path(glue::glue("{theme}/model_id={m_id}/"))) |>
    collect() |>
    distinct(site_id)

  site_coords <- theme_sites |>
    filter(field_site_id %in% model_sites$site_id) |>
    distinct(field_site_id, field_longitude, field_latitude)

  site_coords$site_lat_lon <- lapply(1:nrow(site_coords), function(i) c(site_coords$field_longitude[i], site_coords$field_latitude[i]))

  return(site_coords$site_lat_lon)
}

build_forecast_scores <- function(table_schema,
                           table_description,
                           start_date,
                           end_date,
                           id_value,
                           description_string,
                           about_string,
                           about_title,
                           theme_title,
                           model_documentation,
                           destination_path,
                           description_path,
                           aws_download_path,
                           model_metadata_path
){

  aws_asset_link <- paste0("s3://anonymous@bio230014-bucket01/",
                           aws_download_path,
                           "?endpoint_override=sdsc.osn.xsede.org")

  forecast_score <- list(
    "id" = id_value,
    "description" = description_string,
    "stac_version"= "1.0.0",
    "license"= "CC0-1.0",
    "stac_extensions"= list("https://stac-extensions.github.io/scientific/v1.0.0/schema.json",
                            "https://stac-extensions.github.io/item-assets/v1.0.0/schema.json",
                            "https://stac-extensions.github.io/table/v1.2.0/schema.json"),
    'type' = 'Collection',
    'links' = c(generate_model_items(),
                list(
                  list(
                    "rel" = "parent",
                    "type"= "application/json",
                    "href" = '../collection.json'
                  ),
                  list(
                    "rel" = "root",
                    "type" = "application/json",
                    "href" = '../collection.json'
                  ),
                  list(
                    "rel" = "self",
                    "type" = "application/json",
                    "href" = 'collection.json'
                  ),
                  list(
                    "rel" = "cite-as",
                    "href" = "https://doi.org/10.1002/fee.2616"
                  ),
                  list(
                    "rel" = "about",
                    "href" = about_string,
                    "type" = "text/html",
                    "title" = about_title
                  ),
                  list(
                    "rel" = "describedby",
                    "href" = "https://projects.ecoforecast.org/neon4cast-dashboard/",
                    "title" = "NEON Ecological Forecast Challenge Dashboard",
                    "type" = "text/html"
                  )
                )),
    "title" = theme_title,
    "extent" = list(
      "spatial" = list(
        'bbox' = list(list(-149.6106,
                           18.1135,
                           -66.7987,
                           68.6698))
      ),
      "temporal" = list(
        'interval' = list(list(
          paste0(start_date,"T00:00:00Z"),
          paste0(end_date,"T00:00:00Z"))
        ))
    ),
    "table_columns" = stac4cast::build_table_columns(table_schema, table_description),
    'assets' = list(
      # 'data' = list(
      #   "href"= model_documentation,
      #   "type"= "text/csv",
      #   "roles" = list('data'),
      #   "title"= "NEON Field Site Metadata",
      #   "description"= readr::read_file(model_metadata_path)
      # ),
      'data' = list(
        "href" = aws_asset_link,
        "type"= "application/x-parquet",
        "title"= 'Database Access',
        "roles" = list('data'),
        "description"= readr::read_file(description_path)
      )
    )
  )


  dest <- destination_path
  json <- file.path(dest, "collection.json")

  jsonlite::write_json(forecast_score,
                       json,
                       pretty=TRUE,
                       auto_unbox=TRUE)
  stac4cast::stac_validate(json)
}


build_theme <- function(start_date,end_date, id_value, theme_description, theme_title, destination_path, thumbnail_link, thumbnail_title){

  theme <- list(
    "id" = id_value,
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
        "href"= "https://projects.ecoforecast.org/neon4cast-docs/",
        "type"= "text/html",
        "title"= "NEON Forecast Challenge Documentation"
      ),
      list(
        "rel"= "describedby",
        "href"= "https://projects.ecoforecast.org/neon4cast-dashboard/",
        "title"= "NEON Forecast Challenge Dashboard",
        "type"= "text/html"
      )
    ),
    "title"= theme_title,
    'assets' = list(
      'thumbnail' = list(
        "href"= thumbnail_link,
        "type"= "image/JPEG",
        "roles" = list('thumbnail'),
        "title"= thumbnail_title
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
    "description" = theme_description,
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


  dest <- destination_path
  json <- file.path(dest, "collection.json")

  jsonlite::write_json(theme,
                       json,
                       pretty=TRUE,
                       auto_unbox=TRUE)
  stac4cast::stac_validate(json)
}
