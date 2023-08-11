# AQUATICS
print('running aquatics')
# forecast
source('stac/aquatics/forecasts/models/models.R')
source('stac/aquatics/forecasts/forecast.R')

# scores
source('stac/aquatics/scores/models/models.R')
source('stac/aquatics/scores/scores.R')

source('stac/aquatics/aquatics.R')


## TERRESTRIAL
# Pending

## BEETLES
# Pending

## PHENOLOGY
# Pending

## NOAA
# Pending

## TICKS
# Pending

## SITES
print('building NEON sites')
source('stac/sites/sites.R')

### BUILD CATALOG JSON
print('building NEON sites')
source('stac/catalog.R')
