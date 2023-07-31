library(tidyverse)
library(arrow)
library(stac4cast)
library(reticulate)

source('R/stac_functions.R')

#get model ids
s3 <- s3_bucket("neon4cast-inventory", endpoint_override="data.ecoforecast.org", anonymous = TRUE)
paths <- open_dataset(s3$path("neon4cast-scores")) |> collect()
models_df <- paths |> filter(...1 == "parquet", ...2 == "aquatics") |> distinct(...3)

aquatic_models <- models_df |>
  tidyr::separate(...3, c('name','model.id'), "=")


## READ IN MODEL METADATA

neon_docs <- read_csv('NEON_Challenge_Registration_2023-05-23.csv')


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

neon_docs <- neon_docs |>
  filter(Theme == 'Aquatic Ecosystems') |>
  select(`First Name`:`Email address`,
         `team-name`,
         `Team Member 2 - First Name` :`Team Member 10 - Email`,
         Team.Category:`model-uncertainty`)

names(neon_docs) <- new_columns

neon_docs <- neon_docs |>
  mutate(model.description = ifelse(is.na(model.description),'',model.description))

s3_df <- get_grouping(s3, "aquatics")


info_extract <- arrow::s3_bucket("neon4cast-scores/parquet/", endpoint_override = "data.ecoforecast.org", anonymous = TRUE)


## loop over model ids and extract components if present in metadata table
for (m in aquatic_models$model.id[1:2]){
  print(m)
  model_date_range <- s3_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
  model_min_date <- model_date_range$`min(date)`
  model_max_date <- model_date_range$`max(date)`

  model_var_site_info <- generate_vars_sites(m_id = m, theme = 'aquatics')
  # print(model_var_site_info[[1]])
  # print(model_var_site_info[[2]])

  if (m %in% neon_docs$model.id){
    print('has metadata')

    idx = which(neon_docs$model.id == m)

    build_model(model_id = neon_docs$model.id[idx],
                team_name = neon_docs$team.name[idx],
                model_description = neon_docs[idx,'model.description'][[1]],
                start_date = model_min_date,
                end_date = model_max_date,
                use_metadata = TRUE,
                var_values = model_var_site_info[[1]],
                var_keys = model_var_site_info[[3]][[1]],
                site_values = model_var_site_info[[2]],
                model_documentation = neon_docs,
                destination_path = "stac/aquatics/scores/models/",
                description_path = "stac/aquatics/scores/models/asset-description.Rmd",
                aws_download_path = 'neon4cast-scores/parquet/aquatics',
                theme_title = "Aquatics Scores",
                collection_name = 'scores')
  } else{

    build_model(model_id = m,
                team_name = 'pending',
                model_description = 'pending',
                start_date = model_min_date,
                end_date = model_max_date,
                use_metadata = FALSE,
                var_values = model_var_site_info[[1]],
                var_keys = model_var_site_info[[3]][[1]],
                site_values = model_var_site_info[[2]],
                model_documentation = neon_docs,
                destination_path = "stac/aquatics/scores/models/",
                description_path = "stac/aquatics/scores/models/asset-description.Rmd",
                aws_download_path = 'neon4cast-scores/parquet/aquatics',
                theme_title = "Aquatics Scores",
                collection_name = 'scores')
  }

  rm(model_var_site_info)
}
