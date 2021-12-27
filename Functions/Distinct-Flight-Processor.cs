using System;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
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
        public string flight { get; set; }
    }
    public class Distinct_Flight_Processor
    {
        private static readonly string _databaseId = "Aircraft";
        private static readonly string _containerId = "flights";

        [FunctionName("Distinct-Flight-Processor")]
        public void Run([TimerTrigger("* * * */1 * *",RunOnStartup=true)]TimerInfo myTimer, ILogger log)
        {
            CosmosClient cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("cloudyaircraftdata_DOCUMENTDB"));
            var db = cosmosClient.GetDatabase(_databaseId);
            var container = db.GetContainer(_containerId);
            string query = Environment.GetEnvironmentVariable("queryDefinition");
            if (string.IsNullOrEmpty(query)) query = "SELECT distinct(c.flight) FROM c where c.Timestamp = 1634905178.5 group by c.flight, c.Timestamp";
            
            using (FeedIterator<DistinctFlight> feedIterator = container.GetItemQueryIterator<DistinctFlight>(query))
            {
                while (feedIterator.HasMoreResults)
                {
                    foreach(var item in feedIterator.ReadNextAsync().GetAwaiter().GetResult())
                    {
                        Console.WriteLine(item.flight);
                    }
                }
            }

        }
    }
}
