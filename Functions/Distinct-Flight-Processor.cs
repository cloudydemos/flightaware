using System;
using System.Collections.Generic;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Cosmos;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace CloudyDemos.Aircraft
{
    public class DistinctFlight
    {
        public string id { get; set; }
        public int count { get; set; }
        public double last_seen { get; set; }
        public double closestDistanceInMetresFromMyLocation { get; set; }
    }
    public class Distinct_Flight_Processor
    {
        private static readonly string _databaseId = "Aircraft";
        private static readonly string _flightsContainerId = "flights";
        private static readonly string _flightSpotterContainerId = "flight-spotter";
        private static Microsoft.Azure.Cosmos.CosmosClient cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("cloudyaircraftdata_DOCUMENTDB"));

        [FunctionName("Distinct-Flight-Processor")]
        public void Run([TimerTrigger("0 0 8 * * *")]TimerInfo myTimer, ILogger log)
        {
            // Timer goes off each day at 8am UTC (2am CST)
            int counter = 0;
            try {
                var db = cosmosClient.GetDatabase(_databaseId);
                var flightsContainer = db.GetContainer(_flightsContainerId);
                var flightSpotterContainer = db.GetContainer(_flightSpotterContainerId);
                string query = Environment.GetEnvironmentVariable("queryDefinition");
                if (string.IsNullOrEmpty(query)) query = "SELECT c.flight as id, COUNT(c.flight) as count, max(c.Timestamp) as last_seen, MIN(ST_DISTANCE({\"type\": \"Point\", \"coordinates\":[-95.80341, 29.75959]}, c.Location)) as closestDistanceInMetresFromMyLocation FROM c group by c.flight";
                
                using (FeedIterator<DistinctFlight> feedIterator = flightsContainer.GetItemQueryIterator<DistinctFlight>(query))
                {
                    while (feedIterator.HasMoreResults)
                    {
                        foreach(var item in feedIterator.ReadNextAsync().GetAwaiter().GetResult())
                        {
                            flightSpotterContainer.UpsertItemAsync<DistinctFlight>(item).GetAwaiter().GetResult();
                            counter++;
                            log.LogInformation(string.Format("({0}){1} Item Upserted", counter, item.id));
                        }
                    }
                }
            } catch (Exception ex) {
                log.LogError(ex, ex.Message);
            }
            log.LogInformation(string.Format("{0} Items Upserted", counter));
        }
    }
}
