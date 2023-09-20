
generate_vars_sites <- function(m_id, theme, max_date, bucket){

  info_df <- duckdbfs::open_dataset(glue::glue("s3://anonymous@neon4cast-forecasts/parquet/{theme}/model_id={m_id}/reference_datetime={max_date}?endpoint_override=data.ecoforecast.org")) |>
    #distinct(site_id) |>
    collect()

  if ('siteID' %in% names(info_df)){
    info_df <- info_df |>
      rename(site_id = siteID)
  }

  vars_vector <- sort(unique(info_df$variable))
  sites_vector <- sort(unique(info_df$site_id))

  vars_list <- as.list(sort(unique(info_df$variable)))
  sites_list <- as.list(sort(unique(info_df$site_id)))

  # output_vectors <- c(paste(vars_vector, collapse = ', '),
  #                  paste(sites_vector, collapse = ', '))

  output_list <- list(vars_list,sites_list)


  # FIND SITE BOUNDING BOX / COORDINATES

  #theme_select <- glue::glue('{theme}')

  theme_sites <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv", col_types = cols()) |>
    dplyr::filter(UQ(sym(theme)) == 1)

  if (is.null(m_id)){ ## use for forecast/scores level

    if (bucket == 'Forecasts'){
      bucket_sites <- read_csv(glue::glue('stac/{theme}/forecasts/all_forecast_sites.csv'))
    } else if (bucket == 'Scores'){
      bucket_sites <- read_csv(glue::glue('stac/{theme}/scores/all_scores_sites.csv'))
    } else {
      stop("Bucket name error. Must be 'Forecasts' or 'Scores'")
    }

    site_coords <- theme_sites |>
      filter(field_site_id %in% bucket_sites$site_id) |>
      distinct(field_site_id, field_longitude, field_latitude)

    #site_coords$site_lat_lon <- lapply(1:nrow(site_coords), function(i) c(site_coords$field_longitude[i], site_coords$field_latitude[i]))

    # save bounding box info
    site_object <- c(min(site_coords$field_longitude), min(site_coords$field_latitude), max(site_coords$field_longitude), max(site_coords$field_latitude))

    full_object <- list(vars_vector, sites_vector, output_list, site_object)

    return(full_object)

  }else{ # use for model level
    #model_sites <- duckdbfs::open_dataset(glue::glue("s3://anonymous@neon4cast-forecasts/parquet/{theme}/model_id={m_id}/reference_datetime={max_date}?endpoint_override=data.ecoforecast.org"))

    if (theme == 'ticks'){
      # model_sites <- arrow::open_dataset(info_extract$path(glue::glue("{theme}/model_id={m_id}/"))) |>
      #   collect()

      if('siteID' %in% names(info_df)){
        info_df <- info_df |>
          distinct(Site_ID) |>
          collect() |>
          rename(site_id = siteID)
      }else{
        info_df <- info_df |>
          distinct(site_id) |>
          collect()
      }
    }else{
      info_df <- info_df |>
        distinct(site_id) |>
        collect()
    }

    site_coords <- theme_sites |>
      filter(field_site_id %in% info_df$site_id) |>
      distinct(field_site_id, field_longitude, field_latitude)

    site_coords$site_lat_lon <- lapply(1:nrow(site_coords), function(i) c(site_coords$field_longitude[i], site_coords$field_latitude[i]))

    full_object <- list(vars_vector, sites_vector, output_list, site_coords$site_lat_lon, info_df$site_id)

    return(full_object)
  }
}

#   full_object <- list(vars_vector, sites_vector, output_list)
#
#   return(full_object)
# }


#
#
# get_site_coords <- function(theme, bucket, m_id, max_date){
#
#   theme_select <- glue::glue('{theme}')
#
#   theme_sites <- read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv", col_types = cols()) |>
#     dplyr::filter(UQ(sym(theme_select)) == 1)
#
#   if (is.null(m_id)){
#
#     if (bucket == 'Forecasts'){
#       bucket_sites <- read_csv(glue::glue('stac/{theme}/forecasts/all_forecast_sites.csv'))
#     } else if (bucket == 'Scores'){
#       bucket_sites <- read_csv(glue::glue('stac/{theme}/scores/all_scores_sites.csv'))
#     } else {
#       stop("Bucket name error. Must be 'Forecasts' or 'Scores'")
#     }
#
#     site_coords <- theme_sites |>
#       filter(field_site_id %in% bucket_sites$site_id) |>
#       distinct(field_site_id, field_longitude, field_latitude)
#
#     #site_coords$site_lat_lon <- lapply(1:nrow(site_coords), function(i) c(site_coords$field_longitude[i], site_coords$field_latitude[i]))
#
#     bbox_object <- c(min(site_coords$field_longitude), min(site_coords$field_latitude), max(site_coords$field_longitude), max(site_coords$field_latitude))
#
#     return(bbox_object)
#
#   }else{
#     model_sites <- duckdbfs::open_dataset(glue::glue("s3://anonymous@neon4cast-forecasts/parquet/{theme}/model_id={m_id}/reference_datetime={max_date}?endpoint_override=data.ecoforecast.org"))
#
#     if (theme == 'ticks'){
#       # model_sites <- arrow::open_dataset(info_extract$path(glue::glue("{theme}/model_id={m_id}/"))) |>
#       #   collect()
#
#       if('siteID' %in% names(model_sites)){
#         model_sites <- model_sites |>
#           distinct(Site_ID) |>
#           collect() |>
#           rename(site_id = siteID)
#       }else{
#         model_sites <- model_sites |>
#           distinct(site_id) |>
#           collect()
#       }
#     }else{
#       model_sites <- model_sites |>
#         distinct(site_id) |>
#         collect()
#     }
#
#     site_coords <- theme_sites |>
#       filter(field_site_id %in% model_sites$site_id) |>
#       distinct(field_site_id, field_longitude, field_latitude)
#
#     site_coords$site_lat_lon <- lapply(1:nrow(site_coords), function(i) c(site_coords$field_longitude[i], site_coords$field_latitude[i]))
#
#     return(list(site_coords$site_lat_lon, model_sites$site_id))
#   }
#
# }
