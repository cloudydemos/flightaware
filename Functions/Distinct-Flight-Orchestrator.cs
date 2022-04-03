using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using System;
using System.Text.Json;
using Azure.Storage.Queues; // Namespace for Queue storage types
using Azure.Storage.Queues.Models; // Namespace for PeekedMessage

namespace CloudyDemos.Aircraft
{
     public class DistinctFlight
    {
        public string id { get; set; }
        public int count { get; set; }
        public double last_seen { get; set; }
        public double closestDistanceInMetresFromMyLocation { get; set; }
    }
    public static class Distinct_Flight_Orchestrator
    {
        private const string _databaseId = "Aircraft";
        private const string _flightsContainerId = "flights";
        private const string _flightSpotterContainerId = "flight-spotter";
        private const string _unformattedQuery = "SELECT c.flight as id, COUNT(c.flight) as count, max(c.Timestamp) as last_seen, MIN(ST_DISTANCE({0}\"type\": \"Point\", \"coordinates\":[{1}, {2}]{3}, c.Location)) as closestDistanceInMetresFromMyLocation FROM c where c.flight = \"{4}\" group by c.flight";

        public static CosmosClient cosmosClient { get {
            if (_cosmosClient is null)
                _cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("cloudyaircraftdata_DOCUMENTDB"));
            return _cosmosClient;
        }}
        public static Database database { get {
            if (_database is null)
                cosmosClient.GetDatabase(_databaseId);
            return _database;
        }}
        private static CosmosClient _cosmosClient;
        private static Database _database = cosmosClient.GetDatabase(_databaseId);
        private static Container flightsContainer = database.GetContainer(_flightsContainerId);
        private static Container flightSpotterContainer = database.GetContainer(_flightSpotterContainerId);

        
        [StorageAccount("AzureWebJobsStorage")]
        [FunctionName("ProcessDistinctFlight")]
        public static void ProcessDistinctFlight([QueueTrigger("distinct-flights")] string message,  ILogger log)
        {
            if(string.IsNullOrEmpty(Environment.GetEnvironmentVariable("MyLocationLatitude")) || string.IsNullOrEmpty(Environment.GetEnvironmentVariable("MyLocationLongitude")))
                throw new Exception ("Missing Envinment variables 'MyLocationLatitude' and/or 'MyLocationLongitude'");

            DistinctFlight distinctFlight = JsonSerializer.Deserialize<DistinctFlight>(message);            
            var db = cosmosClient.GetDatabase(_databaseId);
            var flightsContainer = db.GetContainer(_flightsContainerId);
            var flightSpotterContainer = db.GetContainer(_flightSpotterContainerId);
            string query = string.Format(_unformattedQuery, "{", Environment.GetEnvironmentVariable("MyLocationLatitude"), Environment.GetEnvironmentVariable("MyLocationLongitude"), "}", distinctFlight.id);

            // We should get only one result
            using (FeedIterator<DistinctFlight> feedIterator = flightsContainer.GetItemQueryIterator<DistinctFlight>(query))
                while (feedIterator.HasMoreResults)
                    foreach(DistinctFlight item in feedIterator.ReadNextAsync().GetAwaiter().GetResult())
                        flightSpotterContainer.UpsertItemAsync<DistinctFlight>(item);
        }
        [StorageAccount("AzureWebJobsStorage")]
        [FunctionName("GetDistinctFlights")]
        public static async Task GetDistinctFlights([QueueTrigger("distinct-flight-processor")] string message,  ILogger log)
        {
             try {

                log.LogTrace(string.Format("Initiating GetDistinctFlights in response to message: {0}", message));
                string connectionString = Environment.GetEnvironmentVariable("AzureWebJobsStorage");
                string queueName = Environment.GetEnvironmentVariable("queueName");
                if(string.IsNullOrEmpty(queueName)) queueName = "distinct-flights";
                string query = "SELECT DISTINCT c.flight as id, 0 as count, 0 as last_seen, 0 as closestDistanceInMetresFromMyLocation FROM c";
                List<DistinctFlight> distinctFlights = new List<DistinctFlight>();

                // Instantiate a QueueClient which will be used to create and manipulate the queue
                QueueClient queueClient = new QueueClient(connectionString, queueName);
                // Create the queue
                await queueClient.CreateIfNotExistsAsync();
                QueueProperties properties = await queueClient.GetPropertiesAsync();

                // Make sure there are no messages on the queue
                if(properties.ApproximateMessagesCount > 0) {
                    log.LogError(string.Format("GetDistinctFlights Executed but found {0} messages on the queue - aborting.", properties.ApproximateMessagesCount));
                    return;
                }
                
                // Get all the unique flights
                using (FeedIterator<DistinctFlight> feedIterator = flightsContainer.GetItemQueryIterator<DistinctFlight>(query))
                {
                    while (feedIterator.HasMoreResults)
                    {
                        foreach(var item in feedIterator.ReadNextAsync().GetAwaiter().GetResult())
                        {
                            distinctFlights.Add(item);
                        }
                    }
                }
                log.LogInformation(string.Format("Retrieved {0} Flights from the '{1}' container to be upserted into the '{2}' container.", distinctFlights.Count, _flightsContainerId, _flightSpotterContainerId));
            
                // Add message that does not expire
                distinctFlights.ForEach(delegate(DistinctFlight distinctFlight) { queueClient.SendMessageAsync(JsonSerializer.Serialize<DistinctFlight>(distinctFlight), default, TimeSpan.FromSeconds(-1), default); });
                distinctFlights = null;

            } catch (Exception ex) {
                log.LogError(ex, ex.Message);
            }
        }

        [FunctionName("DistinctFlightTimer")]
        [return: Queue("distinct-flight-processor")]
        [StorageAccount("AzureWebJobsStorage")]
        //public static string DistinctFlightTimer([TimerTrigger("0 0 8 * * *")]TimerInfo timerInfo,  ILogger log)
        public static string DistinctFlightTimer([TimerTrigger("0 * * * * *")]TimerInfo timerInfo,  ILogger log)
        {
            QueueClient queueClient = new QueueClient(Environment.GetEnvironmentVariable("AzureWebJobsStorage"), "distinct-flight-processor");
            if (queueClient.CreateIfNotExists() is null) {
                QueueProperties properties = queueClient.GetProperties();
                if(properties.ApproximateMessagesCount > 0) throw new Exception("GetDistinctFlights still processing.");
            }
            
            return string.Format("DistinctFlightTimer Executed at {0} {1}", DateTime.UtcNow.ToShortDateString(), DateTime.UtcNow.ToShortTimeString());
        }        
    }
}