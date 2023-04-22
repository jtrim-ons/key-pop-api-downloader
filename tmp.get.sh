#!/bin/bash

# curl -o dims.json 'https://api.dp.aws.onsdigital.uk/v1/population-types/UR/dimensions'

curl -o file.json 'https://api.beta.ons.gov.uk/v1/population-types/UR/census-observations?area-type=ltla,E07000221&dimensions=health_in_general,highest_qualification,disability_3a'

