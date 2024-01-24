WITH helper AS (    
    SELECT
    train_id,
    arrival_schedule,
    departure_schedule,
    arrival_lateness as arrival_lateness_delta,
    departure_lateness as departure_lateness_delta,
    CASE WHEN arrival_lateness_delta > 0 AND station_name = 'PSL' THEN arrival_lateness_delta ELSE 0 END as arrival_lateness,
    CASE WHEN arrival_lateness_delta < 0 AND station_name = 'PSL' THEN arrival_lateness_delta ELSE 0 END as arrival_earliness,
    CASE WHEN departure_lateness_delta > 0 AND station_name = 'KAJ' THEN departure_lateness_delta ELSE 0 END as departure_lateness,
    CASE WHEN departure_lateness_delta < 0 AND station_name = 'KAJ' THEN departure_lateness_delta ELSE 0 END as departure_earliness,
    FROM {{ ref('f_stop') }}
    LEFT JOIN {{ ref('d_train') }} USING (train_id)
    WHERE regexp_matches(ARRAY_TO_STRING(stop_names, ','), '^KAJ(,.*)*PSL$') AND train_category LIKE 'Long-distance'
    ORDER BY train_id
)

SELECT
train_id,
CASE WHEN arrival_schedule IS null THEN datetrunc('day', departure_schedule) ELSE datetrunc('day', arrival_schedule) END as "day",
-- aggregate functions
MAX(departure_lateness+departure_earliness) as KAJ_departure_timeliness,
MAX(arrival_lateness+arrival_earliness) as HKI_arrival_timeliness,
FROM helper
GROUP BY ALL