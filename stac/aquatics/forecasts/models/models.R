generate_authors <- function(metadata_available){
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


build_model <- function(model_id, team_name, model_description, start_date, end_date, use_metadata, var_values, site_values) {


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
    "description" = glue::glue('# {model_description}

Sites: {site_values}
'),
    "start_datetime" = start_date,
    "end_datetime" = end_date,
    "providers"= c(generate_authors(metadata_available = use_metadata),list(
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
      "title"= 'Database Access',
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

generate_vars_sites <- function(m_id){

  # if (m_id %in%  c('GLEON_JRabaey_temp_physics','GLEON_lm_lag_1day','GLEON_physics','USGSHABs1','air2waterSat_2','fARIMA')){
  #   output_info <- c('pending','pending')
  # } else{

  # do this for each theme / model
  info_df <- arrow::open_dataset(info_extract$path(glue::glue("aquatics/model_id={m_id}/"))) |>
    #filter(reference_datetime == "2023-06-18")|> #just grab one EM to limit processing
    collect()

  vars <- unique(info_df$variable)
  sites <- unique(info_df$site_id)

  output_info <- c(paste(vars, collapse = ', '),
                   paste(sites, collapse = ', '))
  #}
  return(output_info)
  }

## read in model documentation and only grab models for Aquatics theme
library(tidyverse)
library(arrow)
library(stac4cast)
library(reticulate)

#get model ids
s3 <- s3_bucket("neon4cast-inventory", endpoint_override="data.ecoforecast.org", anonymous = TRUE)
paths <- open_dataset(s3$path("neon4cast-forecasts")) |> collect()
models_df <- paths |> filter(...1 == "parquet", ...2 == "aquatics") |> distinct(...3)

aquatic_models <- models_df |>
  tidyr::separate(...3, c('name','model.id'), "=")


## READ IN MODEL METADATA

model_docs <- read_csv('NEON_Challenge_Registration_2023-05-23.csv')


new_columns <- c('first.name.one',
                 'last.name.one',
                 'affiliation.one',
                 'email.one',
                 'team.name',
                 'first.name.two',
                 'last.name.two',
                 'affiliation.two',
                 'email.two',
                 'first.name.three',
                 'last.name.three',
                 'affiliation.three',
                 'email.two.three',
                 'first.name.four',
                 'last.name.four',
                 'affiliation.four',
                 'email.four',
                 'first.name.five',
                 'last.name.five',
                 'affiliation.five',
                 'email.five',
                 'first.name.six',
                 'last.name.six',
                 'affiliation.six',
                 'email.six',
                 'first.name.seven',
                 'last.name.seven',
                 'affiliation.seven',
                 'email.seven',
                 'first.name.eight',
                 'last.name.eight',
                 'affiliation.eight',
                 'email.eight',
                 'first.name.nine',
                 'last.name.nine',
                 'affiliation.nine',
                 'email.nine',
                 'first.name.ten',
                 'last.name.ten',
                 'affiliation.ten',
                 'email.ten',
                 'team.category',
                 'theme',
                 'model.id',
                 'model.description',
                 'model.uncertainty'
)

model_docs <- model_docs |>
  filter(Theme == 'Aquatic Ecosystems') |>
  select(`First Name`:`Email address`,
         `team-name`,
         `Team Member 2 - First Name` :`Team Member 10 - Email`,
         Team.Category:`model-uncertainty`)

names(model_docs) <- new_columns

model_docs <- model_docs |>
  mutate(model.description = ifelse(is.na(model.description),'',model.description))

#model_docs <- read_csv('NEON_Challenge_Registration_2023-05-23.csv')

# model_docs <- read_csv('NEON_Challenge_Registration_2023-05-23.csv')
#
# model_docs <- model_docs |>
#   filter(Theme == 'Aquatic Ecosystems') |>
#   select(`team-name`, `First Name`, `Last Name`, `Email address`, `model-id`, `model-description`) |>
#   rename(team.name = `team-name`, first.name = `First Name`, last.name = `Last Name`, email = `Email address`,
#          model.id = `model-id`, model.description = `model-description`) |>
#   mutate(model.description = ifelse(is.na(model.description),'',model.description))

# model_docs <- model_docs |>
#   select(`First Name`:`Email address`,
#          `team-name`,
#          `Team Member 2 - First Name` :`Team Member 10 - Email`,
#          Team.Category:`model-uncertainty`)
#
# names(model_docs) <- new_columns

## READ S3 INVENTORY FOR DATES
# s3_inventory <- arrow::s3_bucket("neon4cast-inventory",
#                           endpoint_override = "data.ecoforecast.org",
#                           anonymous = TRUE)

s3_df <- get_grouping(s3, "aquatics")


info_extract <- arrow::s3_bucket("neon4cast-scores/parquet/", endpoint_override = "data.ecoforecast.org", anonymous = TRUE)

## loop over model ids and extract components if present in metadata table
for (m in aquatic_models$model.id[1:2]){
  print(m)
  model_date_range <- s3_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  model_var_site_info <- generate_vars_sites(m_id = m)
  # print(model_var_site_info[[1]])
  # print(model_var_site_info[[2]])

  if (m %in% model_docs$model.id){
    print('has metadata')

    idx = which(model_docs$model.id == m)

    build_model(model_id = model_docs$model.id[idx],
                team_name = model_docs$team.name[idx],
                model_description = model_docs[idx,'model.description'][[1]],
                start_date =model_min_date,
                end_date = model_max_date,
                use_metadata = TRUE,
                var_values = model_var_site_info[1],
                site_values = model_var_site_info[2])
  } else{

    build_model(model_id = m,
                team_name = 'pending',
                model_description = 'pending',
                model_min_date,
                model_max_date,
                use_metadata = FALSE,
                var_values = model_var_site_info[1],
                site_values = model_var_site_info[2])
  }

  rm(model_var_site_info)
}
