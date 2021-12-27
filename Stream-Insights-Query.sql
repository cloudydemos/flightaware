WITH AllFlights AS (
SELECT
    RTRIM(aircraftRecords.ArrayValue.flight) as PartitionKey,
    i.now as RowKey,
    CONCAT(i.now, '-', RTRIM(aircraftRecords.ArrayValue.flight)) as id,
    aircraftRecords.ArrayValue.alt_baro as alt_baro,
    aircraftRecords.ArrayValue.alt_geom as alt_geom,
    aircraftRecords.ArrayValue.geom_rate as geom_rate,
    aircraftRecords.ArrayValue.baro_rate as baro_rate,
    aircraftRecords.ArrayValue.category as category,
    aircraftRecords.ArrayValue.gs as gs,
    aircraftRecords.ArrayValue.hex as hex,
    aircraftRecords.ArrayValue.lat as lat,
    aircraftRecords.ArrayValue.lon as lon,
    aircraftRecords.ArrayValue.nav_altitude_mcp as nav_altitude_mcp,
    aircraftRecords.ArrayValue.nav_heading as nav_heading,
    aircraftRecords.ArrayValue.nav_qnh as nav_qnh,
    CAST(aircraftRecords.ArrayValue.squawk as float) as squawk,
    aircraftRecords.ArrayValue.track as track,
    ST_DISTANCE(CreatePoint(29.75959, -95.80341), CreatePoint(aircraftRecords.ArrayValue.lat, aircraftRecords.ArrayValue.lon)) as distanceInMetresFromHome
FROM
    [Iot-Hub] i TIMESTAMP BY DATEADD(second, i.now, '1970-01-01T00:00Z')
CROSS APPLY GetArrayElements(aircraft) AS aircraftRecords
WHERE
    len(RTRIM(aircraftRecords.ArrayValue.flight)) > 1
    AND aircraftRecords.ArrayValue.lon is not null
    AND aircraftRecords.ArrayValue.lat is not null
),
CloseAircraft AS ( 
    SELECT
        PartitionKey as flight,
        distanceInMetresFromHome
    FROM
        [AllFlights] 
    where distanceInMetresFromHome < 1000
)
SELECT
    PartitionKey,
    RowKey,
    alt_baro,
    alt_geom,
    baro_rate,
    category,
    gs,
    hex,
    lat,
    lon,
    nav_altitude_mcp,
    nav_heading,
    nav_qnh,
    squawk,
    track
INTO
    [Aircraft]
FROM
    [AllFlights]

SELECT flight, 
    DateAdd(minute,-3,System.Timestamp()) AS FirstSeen,
    System.Timestamp() AS LastSeen,
    ROUND(Min(distanceInMetresFromHome), 0) as closestDistanceInMetresFromHome,
    COUNT(*) as RecordCount 
INTO
    [Nearby-Aircraft]
 FROM
    [CloseAircraft]
GROUP BY
    flight,
    TumblingWindow(minute, 3)
  
SELECT
    id,
    PartitionKey as flight,
    RowKey as 'Timestamp',
    alt_baro,
    alt_geom,
    baro_rate,
    category,
    gs,
    hex,
    lat,
    lon,
    nav_altitude_mcp,
    nav_heading,
    nav_qnh,
    squawk,
    track
INTO
    [Cosmos]
FROM
    [AllFlights]
PARTITION BY flight

