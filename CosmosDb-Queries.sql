-- Pull all the flights within 1,000m from MyLocation and when they were last seen from the flight-spotter Container:
SELECT c.id, c.closestDistanceInMetresFromMyLocation, TimestampToDateTime (c.last_seen * 1000) AS last_seen FROM c 
where c.closestDistanceInMetresFromMyLocation < 1000
order by c.last_seen desc

-- Show the 10 closest flights and when they were last seen from MyLocation from the flight-spotter Container:
SELECT top 10 c.id, ROUND(c.closestDistanceInMetresFromMyLocation) as closestDistanceInMetresFromMyLocation,
TimestampToDateTime(ROUND(c.last_seen * 1000)) AS last_seen_utc FROM c 
where c.closestDistanceInMetresFromMyLocation < 1000
order by c.closestDistanceInMetresFromMyLocation asc

-- Get all Distinct flights and when last seen from Aircraft container
SELECT c.flight as id, COUNT(c.flight) as count, 
max(c.Timestamp) as last_seen, 
MIN(ST_DISTANCE({"type": "Point", "coordinates":[-95.80341, 29.75959]}, c.Location)) as closestDistanceInMetresFromMyLocation FROM c group by c.flight

-- Get every flight's Full Date and distancein metres from My location from Aircraft Container
SELECT c.flight,
TimestampToDateTime(ROUND(c.Timestamp * 1000)) AS Timestamp, 
ROUND(ST_DISTANCE({"type": "Point", "coordinates":[-95.80341, 29.75959]}, c.Location)) as distanceInMetresFromMyLocation
FROM c
--where c.flight = "AAL1206"
order by c.Timestamp

-- Get a Timestamp for a period of time before now
SELECT ROUND(DateTimeToTimestamp(DateTimeAdd("hh", -2, GetCurrentDateTime()))/1000) AS timestampTwoHoursAgo
SELECT ROUND(DateTimeToTimestamp(DateTimeAdd("minute", -2, GetCurrentDateTime()))/1000) AS timestampTwoMinutesAgo
SELECT ROUND(DateTimeToTimestamp(DateTimeAdd("minute", -3, GetCurrentDateTime()))/1000) AS timestampThreeMinutesAgo
-- Get all flights that have come in recently (have to cut and paste output from queries above)
SELECT * from c where c.Timestamp > 1641095820 and c.Timestamp <  1641095835
SELECT * from c where c.Timestamp > 1641095732