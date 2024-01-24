WITH exploded AS (
    SELECT
    route_sk,
    trainNumber,
    trainCategory,
    operatorUICCode,
    operatorShortCode,
    runningCurrently,
    timetableType,
    unnest(timeTableRows) as timetable_row
FROM {{ source('vr_data_raw', 'vr_data_') }}
),

defined AS (
    SELECT
    * EXCLUDE (timetable_row),
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

numbered AS (
    SELECT
    trainNumber as train_id,
    trainCategory as train_category,
    operatorUICCOde as operator_id,
    operatorShortCode as operator_code,
    timetableType as timetable_type,
    ROW_NUMBER() OVER (
        PARTITION BY train_id
        ORDER BY timetable_struct.scheduledTime
    ) as row_number,
    timetable_struct.stationUICCode as stop_id,
    timetable_struct.stationShortCode as stop_name,
    FROM defined
),

as_list AS (
    SELECT
    train_id,
    train_category,
    operator_id,
    operator_code,
    timetable_type,
    LIST(stop_id) OVER (
        PARTITION BY train_id
        ORDER BY row_number
    ) as list_stop_ids,
    LIST(stop_name) OVER (
        PARTITION BY train_id
        ORDER BY row_number
        ) as list_stop_names
    FROM numbered
    WHERE row_number % 2
)

SELECT
train_id,
train_category,
FIRST(operator_id) as operator_id,
FIRST(operator_code) as operator_code,
FIRST(timetable_type) as timetable_type,
MAX(list_stop_ids) as stop_ids,
MAX(list_stop_names) as stop_names,
ARRAY_TO_STRING(
    MAX(list_stop_names), '-'
    ) as stop_names_string
FROM as_list
GROUP BY ALL