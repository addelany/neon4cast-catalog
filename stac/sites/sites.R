source('R/stac_functions.R')


## READ S3 INVENTORY FOR DATES
s3_inventory <- arrow::s3_bucket("neon4cast-inventory",
                                 endpoint_override = "data.ecoforecast.org",
                                 anonymous = TRUE)

s3_df <- get_grouping(s3_inventory, "aquatics")

s3_df <- s3_df |> filter(model_id != 'null')

theme_max_date <- max(s3_df$date)
theme_min_date <- min(s3_df$date)

build_description <- "This collection contains information to describe the NEON sites included in the forecasting challenge"

build_site_theme(start_date = '2000-01-01',
                 end_date = sys.Date(),
                 id_value = 'efi-sites',
            theme_description = build_description,
            theme_title = 'NEON Sites',
            destination_path = "stac/sites/",
            thumbnail_link = 'https://www.neonscience.org/sites/default/files/styles/max_1300x1300/public/2021-04/2021_04_Graphic_Domain_Map%20Field%20sites_w_rivers_png.png?itok=0_oMxQB8',
            thumbnail_title = 'NEON Sites')
