# Data dictionary
## `fct_flights`
- **pk** (`varchar`): Primary key of the table. This is a composite key comprised of the concatenation of: `dt`, `crs_departure_time`, `airline_iata_code`, `flight_number`
- **dt** (`date`): Date of the flight
- **airline_iata_code** (`varchar`): The IATA code of the airline 
- **flight_number** (`int`): Flight number (without airline code)
- **origin_airport** (`varchar`): IATA code of the origin airport
- **destination_airport** (`varchar`): IATA code of the destination airport
- **crs_departure_time** (`varchar`): Departure time as per the CRS (Computer Reservation System, which provides information on airline schedules, fares and seat availability to travel agencies and allow agents to book seats and issue tickets). This is can be used as the planned departure time
- **departure_time** (`varchar`): Actual departure time
- **crs_arrival_time** (`varchar`): Departure time as pre the CRS (planned arrival time)
- **arrival_time** (`varchar`): Actual arrival time
- **wheels_off_time** (`varchar`): The time at which wheels of the plane left the ground at the departure
- **wheels_on_time** (`varchar`): The time at which the the wheels touched the ground at the arrival
- **air_time** (`int`): Time, in minutes, that the plane was in the air
- **crs_elapsed_time** (`int`): Flight time (min) as per the CRS
- **actual_elapsed_time** (`int`): Actual flight time (min)
- **delay_time** (`int`): Delay time (min)
- **is_delayed** (`boolean`): Boolean that states if the flight was delayed
- **delay_is_carrier** (`boolean`): Booleand that states if the also was the carrier's fault
- **main_delay_reason** (`varchar`): The main delay reason
- **is_cancelled** (`boolean`): Boolean that states if the flight was cancelled
- **cancellation_code** (`varchar`): Cancellation code - More info in [BTS - Technical Directive: On-Time Reporting](https://www.bts.gov/topics/airlines-and-airports/number-23-technical-directive-time-reporting-effective-jan-1-2014)

## `dim_date`
- **dt** (`date`): Date in `yyyy-mm-dd` format
- **year** (`int`): The year of the date
- **month** (`int`): The month of the date
- **day** (`int`): The day of the date
- **quarter** (`int`): The quarter of the date
- **semester** (`int`): The semester of the date

## `dim_airlines`
- **iata_code** (`varchar`): IATA code of the airline
- **icao_code** (`varchar`): ICAO code of the airline
- **name** (`varchar`): Name of the airline
- **callsign** (`varchar`): Callsign of the airline
- **country** (`varchar`): Country from which the airline is
- **active** (`boolean`): If the airline is still operating

## `dim_airports`
- **iata_code** (`varchar`): IATA code of the airport
- **name** (`varchar`): Name of the airport
- **longitude** (`float`): Longitude of the airport
- **latitude** (`float`): Latitude of the airport
- **elevation** (`int`): Elevation (sea level) of the airport
- **gps_code** (`varchar`): GPS code of the airport
- **iso_country** (`varchar`): The ISO code of the country where the airport is located
- **iso_region** (`varchar`): The ISO code of the region where the airport is located
- **municipality** (`varchar`): The municipality where the airport is located
- **type** (`varchar`): The type of the airport (e.g: `small_airport`, `big_airport`, `heliport`)