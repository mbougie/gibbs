﻿----Description: derive the last two columns of extensification_mlra.extensification_price_response after inputting the other columns in manually from excel file


update extensification_mlra.extensification_price_response set expand_from_either = expand_from_pasture + expand_from_crp

update extensification_mlra.extensification_price_response set abandon_to_either = abandon_to_pasture + abandon_to_crp