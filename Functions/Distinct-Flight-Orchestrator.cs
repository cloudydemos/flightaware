using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using System;

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
        private static readonly string _databaseId = "Aircraft";
        private static readonly string _flightsContainerId = "flights";
        private static readonly string _flightSpotterContainerId = "flight-spotter";
        private static string _query = null; 

        public static string Query { get {
            if (_query is null)
                _query = string.IsNullOrEmpty(Environment.GetEnvironmentVariable("queryDefinition")) ? Environment.GetEnvironmentVariable("queryDefinition") : 
                "SELECT c.flight as id, COUNT(c.flight) as count, max(c.Timestamp) as last_seen, MIN(ST_DISTANCE({\"type\": \"Point\", \"coordinates\":[-95.80341, 29.75959]}, c.Location)) as closestDistanceInMetresFromMyLocation FROM c where c.flight = \"{0}\" group by c.flight";
            return _query;
        }}

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

        [FunctionName("Distinct-Flight-Orchestrator")]
        public static async Task RunOrchestrator([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var parallelTasks = new List<Task>();

            DistinctFlight[] workBatch = await context.CallActivityAsync<DistinctFlight[]>("GetFlights", null);

            for (int i = 0; i < workBatch.Length; i++)
            {
                Task task = context.CallActivityAsync("UpdateDistinctFlight", workBatch[i]);
                parallelTasks.Add(task);
            }

            await Task.WhenAll(parallelTasks);
                   
        }

        [FunctionName("GetFlights")]
        public static DistinctFlight[] GetFlights([ActivityTrigger] string name, ILogger log)
        {
            string query = "SELECT DISTINCT c.flight as id, 0 as count, 0 as last_seen, 0 as closestDistanceInMetresFromMyLocation FROM c";
            List<DistinctFlight> distinctFlights = new List<DistinctFlight>();
            
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
            return distinctFlights.ToArray();
        }

        [FunctionName("UpdateDistinctFlight")]
        public static void UpdateDistinctFlight([ActivityTrigger] DistinctFlight distinctFlight, ILogger log)
        {   
                var db = cosmosClient.GetDatabase(_databaseId);
                var flightsContainer = db.GetContainer(_flightsContainerId);
                var flightSpotterContainer = db.GetContainer(_flightSpotterContainerId);
                string query;

                try {
                    query = string.Format(Query, distinctFlight.id);
                } catch (FormatException) {
                    log.LogError("UpdateDistinctFlight Query Exception: " + Query);
                    return;
                }
                
                // We should get only one result
                using (FeedIterator<DistinctFlight> feedIterator = flightsContainer.GetItemQueryIterator<DistinctFlight>(query))
                    while (feedIterator.HasMoreResults)
                        foreach(DistinctFlight item in feedIterator.ReadNextAsync().GetAwaiter().GetResult())
                            flightSpotterContainer.UpsertItemAsync<DistinctFlight>(item).GetAwaiter().GetResult();
        }

        [FunctionName("Distinct-Flight-Orchestrator-Start")]
        public static async Task Start(
            [TimerTrigger("0 0 8 * * *")]TimerInfo timerInfo,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
             // Check if an instance with our ID "Distinct-Flight-Orchestrator" already exists or an existing one stopped running (completed/failed/terminated).
            var existingInstance = await starter.GetStatusAsync("Distinct-Flight-Orchestrator");
            if (existingInstance == null 
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Completed 
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Failed 
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Terminated)
            {
                // An instance with our ID "Distinct-Flight-Orchestrator" doesn't exist or an existing one stopped running, create one.
                await starter.StartNewAsync("Distinct-Flight-Orchestrator", "Distinct-Flight-Orchestrator", timerInfo);
                log.LogInformation($"Started new instance of Distinct-Flight-Orchestrator.");
            } else {
                log.LogError($"Failed to started new instance of Distinct-Flight-Orchestrator: already running.");
            }
        }
    }
}