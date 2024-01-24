WITH exploded AS (
    SELECT
        route_sk,
        trainNumber,
        UNNEST(timeTableRows) as timetable_row
    FROM {{ source('vr_data_raw', 'vr_data_raw') }}
),

defined AS (
    SELECT 
        route_sk,
        trainNumber,
        timetable_row::STRUCT(
            stationShortCode VARCHAR, 
            stationUICCode INTEGER, 
            countryCode VARCHAR, 
            "type" VARCHAR, 
            trainStopping BOOLEAN, 
            commercialStop BOOLEAN, 
            commercialTrack VARCHAR,
            cancelled BOOLEAN,
            scheduledTime TIMESTAMP, 
            liveEstimateTime VARCHAR, 
            estimateSource VARCHAR, 
            unknownDelay BOOLEAN, 
            actualTime VARCHAR, 
            differenceInMinutes BIGINT, 
            causes STRUCT(passengerTerm STRUCT(fi VARCHAR, en VARCHAR, sv VARCHAR), 
            categoryCode VARCHAR, 
            categoryName VARCHAR, 
            validFrom VARCHAR, 
            validTo VARCHAR, 
            id INTEGER, 
            detailedCategoryCode VARCHAR, 
            detailedCategoryName VARCHAR, 
            thirdCategoryCode VARCHAR, 
            thirdCategoryName VARCHAR, 
            "description" VARCHAR, 
            categoryCodeId INTEGER, 
            detailedCategoryCodeId INTEGER, 
            thirdCategoryCodeId INTEGER)[], 
            trainReady STRUCT(source VARCHAR, accepted BOOLEAN, "timestamp" VARCHAR)

        ) as timetable_struct
    FROM exploded
),

flattened AS (
SELECT
    trainNumber,
    md5(route_sk || timetable_struct.stationShortCode) as stop_sk,
    route_sk,
    timetable_struct.*
FROM defined
)

SELECT
    stop_sk,
    FIRST(trainNumber) as train_id,
    FIRST(stationUICCode) as station_id,
    FIRST(stationShortCode) as station_name,
    MAX(CASE WHEN "type" = 'ARRIVAL' THEN differenceInMinutes END) AS arrival_lateness,
    MAX(CASE WHEN "type" = 'DEPARTURE' THEN differenceInMinutes END) AS departure_lateness,
    FIRST(CASE WHEN "type" = 'ARRIVAL' THEN scheduledTime END) AS arrival_schedule,
    FIRST(CASE WHEN "type" = 'DEPARTURE' THEN scheduledTime END) AS departure_schedule,
    MAX(CASE WHEN "type" = 'ARRIVAL' THEN actualTime END) AS arrival_actual,
    MAX(CASE WHEN "type" = 'DEPARTURE' THEN actualTime END) AS departure_actual,
    FLATTEN(LIST(causes)) as lateness_causes

FROM flattened
GROUP BY ALL