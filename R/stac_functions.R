## model level functions

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
                        team_name,
                        model_description,
                        start_date,
                        end_date,
                        use_metadata,
                        var_values,
                        site_values,
                        model_documentation,
                        destination_path,
                        description_path,
                        theme_title) {


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
      "description" = glue::glue('

    model info: {model_description}

    Sites: {site_values}

    Variables: {var_values}
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
      "keywords"= list(
        "Forecasting",
        var_values)
    ),
    "collection"= "forecast",
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
    "assets"= list(
      "parquet_items"= list(
        "href"= paste0("s3://anonymous@",
                       "bio230014-bucket01/neon4cast-forecasts/parquet/",
                       "aquatics/model_id=", model_id,
                       "?endpoint_override=sdsc.osn.xsede.org"),
        "type"= "application/x-parquet",
        "title"= 'Database Access',
        "description"= readr::read_file(description_path)
      )
    )
  )


  dest <- destination_path
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


generate_vars_sites <- function(m_id, theme){

  # if (m_id %in%  c('GLEON_JRabaey_temp_physics','GLEON_lm_lag_1day','GLEON_physics','USGSHABs1','air2waterSat_2','fARIMA')){
  #   output_info <- c('pending','pending')
  # } else{

  # do this for each theme / model
  info_df <- arrow::open_dataset(info_extract$path(glue::glue("{theme}/model_id={m_id}/"))) |>
    #filter(reference_datetime == "2023-06-18")|> #just grab one EM to limit processing
    collect()

  vars <- sort(unique(info_df$variable))
  sites <- sort(unique(info_df$site_id))

  output_info <- c(paste(vars, collapse = ', '),
                   paste(sites, collapse = ', '))
  #}
  return(output_info)
}

