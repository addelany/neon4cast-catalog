library(arrow)
library(tidyverse)

#get model ids
s3 <- s3_bucket("neon4cast-inventory", endpoint_override="data.ecoforecast.org", anonymous = TRUE)
paths <- open_dataset(s3$path("neon4cast-scores")) |> collect()
models_df <- paths |> filter(...1 == "parquet", ...2 == "aquatics") |> distinct(...3)

aquatic_models <- models_df |>
  tidyr::separate(...3, c('name','model.id'), "=")

info_extract <- arrow::s3_bucket("neon4cast-scores/parquet/", endpoint_override = "data.ecoforecast.org", anonymous = TRUE)

theme <- 'aquatics'

# for (m in aquatic_models$model.id){
#
#   info_df <- arrow::open_dataset(info_extract$path(glue::glue("{theme}/model_id={m_id}/"))) |>
#     collect()
#
#   }


info_test <- arrow::open_dataset(info_extract$path(glue::glue("aquatics/model_id=BBTW/"))) |>
  collect()

latest_forecast_date <- max(info_test$reference_datetime)

latest_forecast <- info_test |>
  filter(reference_datetime == latest_forecast_date)

climatology_df <- arrow::open_dataset(info_extract$path(glue::glue("aquatics/model_id=climatology/"))) |>
  filter(reference_datetime == latest_forecast_date,
         datetime %in% latest_forecast$datetime) |>
  collect()

latest_forecast <- info_test |>
  filter(reference_datetime == latest_forecast_date)

latest_forecast$horizon <- as.Date(latest_forecast$datetime) - as.Date(latest_forecast$reference_datetime)


for (site in unique(latest_forecast$site_id)){

  clim_site_df <- climatology_df |>
    filter(site_id == site) |>
    rename(clim_crps = crps) |>
    select(datetime,variable, clim_crps)

  latest_site_df <- latest_forecast |>
    filter(site_id == site) |>
    right_join(clim_site_df, by = c('datetime','variable'))


  # Forecast Plot
  forecast_plot <- ggplot(data = latest_site_df, aes(datetime, mean)) +
    geom_line(color = "steelblue", linewidth = 1) +
    ggplot2::geom_ribbon(aes(ymin = quantile10, ymax = quantile90), alpha = 0.2) +
    labs(title = paste0("Latest Forecast for ", site," (",latest_forecast_date,')'),
         subtitle = "(plots include the mean and the +- 90% CI)",
         y = "Forecast value", x = "Date") +
    facet_grid(variable ~ ., scales = "free_y") +
    theme_bw() +
    theme(plot.title = element_text(hjust = 0.5),
          plot.subtitle=element_text(hjust=0.5))

  # Scores plot
  scores_plot <- ggplot(data = latest_site_df, aes(horizon, crps)) +
    geom_line(color = "steelblue", linewidth = 1) +
    geom_line(aes(y = clim_crps), color = 'darkred', linetype = 'dashed') +
    labs(title = paste0("Latest Scores for ", site," (",latest_forecast_date,')'),
         subtitle = "modeled CRPS score (blue) and the climatology CRPS score (red)",
         y = "CRPS Score", x = "Horizon (Days)") +
    facet_grid(variable ~ ., scale = "free_y") +
    theme_bw() +
    theme(plot.title = element_text(hjust = 0.5),
          plot.subtitle=element_text(hjust=0.5))

  ##save plot
}

scores_plot
