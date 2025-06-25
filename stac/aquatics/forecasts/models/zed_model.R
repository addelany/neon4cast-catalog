
model_function <- function(){
  #test_models <- c(aquatic_models$model.id[1:2], 'tg_arima')
  ## loop over model ids and extract components if present in metadata table
  for (m in c('climatology')){ #aquatic_models$model.id[1:2]){
    print(m)
    model_date_range <- s3_df |> filter(model_id == m) |> dplyr::summarise(min(date),max(date))
    model_min_date <- model_date_range$`min(date)`
    model_max_date <- model_date_range$`max(date)`

    model_var_site_info <- generate_vars_sites(m_id = m, theme = 'aquatics')
    # print(model_var_site_info[[1]])
    # print(model_var_site_info[[2]])

    forecast_sites <- append(forecast_sites,  get_site_coords(theme = 'aquatics', bucket = NULL, m_id = m)[[2]])

    if (m %in% neon_docs$model.id){
      print('has metadata')

      idx = which(neon_docs$model.id == m)

      build_model(model_id = neon_docs$model.id[idx],
                  theme_id = 'aquatics',
                  team_name = neon_docs$team.name[idx],
                  model_description = neon_docs[idx,'model.description'][[1]],
                  start_date = model_min_date,
                  end_date = model_max_date,
                  use_metadata = TRUE,
                  var_values = model_var_site_info[[1]],
                  var_keys = model_var_site_info[[3]][[1]],
                  site_values = model_var_site_info[[2]],
                  model_documentation = neon_docs,
                  destination_path = "stac/aquatics/forecasts/models/",
                  description_path = "stac/aquatics/forecasts/models/asset-description.Rmd",
                  aws_download_path = 'neon4cast-forecasts/parquet/aquatics',
                  theme_title = "Forecasts",
                  collection_name = 'forecasts',
                  thumbnail_image_name = 'latest_forecast.png',
                  table_schema = theme_df,
                  table_description = description_create)
    } else{

      build_model(model_id = m,
                  theme_id = 'aquatics',
                  team_name = 'pending',
                  model_description = 'pending',
                  start_date = model_min_date,
                  end_date = model_max_date,
                  use_metadata = FALSE,
                  var_values = model_var_site_info[[1]],
                  var_keys = model_var_site_info[[3]][[1]],
                  site_values = model_var_site_info[[2]],
                  model_documentation = neon_docs,
                  destination_path = "stac/aquatics/forecasts/models/",
                  description_path = "stac/aquatics/forecasts/asset-description.Rmd",
                  aws_download_path = 'neon4cast-forecasts/parquet/aquatics',
                  theme_title = "Forecasts",
                  collection_name = 'forecasts',
                  thumbnail_image_name = 'latest_forecast.png',
                  table_schema = theme_df,
                  table_description = description_create)
    }

    rm(model_var_site_info)
  }


}# end of model func
