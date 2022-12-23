# Data dictionary
## `fct_flights`
- **pk** (`varchar`): Primary key of the table
- **dt** (`date`): 
- **airline_iata_code** (`varchar`): 
- **flight_number** (`int`): 
- **origin_airport** (`varchar`): 
- **destination_airport** (`varchar`): 
- **crs_departure_time** (`varchar`): 
- **departure_time** (`varchar`): 
- **crs_arrival_time** (`varchar`): 
- **arrival_time** (`varchar`): 
- **wheels_off_time** (`varchar`): 
- **wheels_on_time** (`varchar`): 
- **air_time** (`int`): 
- **crs_elapsed_time** (`int`): 
- **actual_elapsed_time** (`int`): 
- **delay_time** (`int`): 
- **is_delayed** (`boolean`): 
- **delay_is_carrier** (`boolean`): 
- **main_delay_reason** (`varchar`): 
- **is_cancelled** (`boolean`): 
- **cancellation_code** (`varchar`): 

## `dim_date`
- **dt** (`date`):
- **year** (`int`):
- **month** (`int`):
- **day** (`int`):
- **quarter** (`int`):
- **semester** (`int`):

## `dim_airlines`
- **iata_code** (`varchar`): 
- **icao_code** (`varchar`): 
- **name** (`varchar`): 
- **callsign** (`varchar`): 
- **country** (`varchar`): 
- **active** (`boolean`): 

## `dim_airports`
- **iata_code** (`varchar`): 
- **name** (`varchar`): 
- **longitude** (`float`): 
- **latitude** (`float`): 
- **elevation** (`int`): 
- **gps_code** (`varchar`): 
- **iso_country** (`varchar`): 
- **iso_region** (`varchar`): 
- **municipality** (`varchar`): 
- **type** (`varchar`): 