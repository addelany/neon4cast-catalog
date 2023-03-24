from stac_validator import stac_validator

#stac = stac_validator.StacValidate("https://raw.githubusercontent.com/radiantearth/stac-spec/master/collection-spec/json-schema/collection.json")
#stac = stac_validator.StacValidate("https://raw.githubusercontent.com/stac-utils/pystac/main/tests/data-files/examples/0.9.0/collection-spec/examples/landsat-collection.json")

stac = stac_validator.StacValidate("https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/v1/collection/aquatics/output.json")
#stac = stac_validator.StacValidate("https://raw.githubusercontent.com/addelany/neon4cast-catalog/main/stac/v1/collection/aquatics/output_remove_array.json")

stac.run()
print(stac.message)

