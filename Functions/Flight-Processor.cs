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
        public class Location
    {
        public string type { get; set; }
        public List<double> coordinates { get; set; }
    }

    public class Flight
    {
        // Partition Key
        public string flight { get; set; }
        public string Timestamp { get; set; }
        public double? lat { get; set; }
        public double? lon { get; set; }
        public string id { get; set; }
        public Location location { get; set; }
    }

    public static class FlightProcessor
    {
        private static readonly string _databaseId = "Aircraft";
        private static readonly string _containerId = "flights";
        private static CosmosClient cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("cloudyaircraftdata_DOCUMENTDB"));

        //[FunctionName("FlightProcessor")]
        public static void Run([CosmosDBTrigger(
            databaseName: "Aircraft",
            collectionName: "flights",
            ConnectionStringSetting = "cloudyaircraftdata_DOCUMENTDB",
            LeaseCollectionName = "leases",
            CreateLeaseCollectionIfNotExists = true)]IReadOnlyList<Document> input,
            ILogger log)
        {
            if (input != null && input.Count > 0)
            {
                try {
                    foreach(Document doc in input) {

                    Flight flight = JsonConvert.DeserializeObject<Flight>(doc.ToString());

                    if(flight.location != null || flight.lat is null || flight.lon is null) 
                        continue;

                    var db = cosmosClient.GetDatabase(_databaseId);
                    var container = db.GetContainer(_containerId);
                    
                    // Convert lat & lon to a GeoSpatial Point so we can do distance calcs on it (https://docs.microsoft.com/en-us/azure/cosmos-db/sql/sql-query-geospatial-query)
                    List<PatchOperation> patchOperations = new List<PatchOperation>();
                    Location location = new Location { type = "Point", coordinates = new List<double>() { flight.lon.GetValueOrDefault(), flight.lat.GetValueOrDefault() } };
                    patchOperations.Add(PatchOperation.Add("/Location", location));
                    patchOperations.Add(PatchOperation.Remove("/lat"));
                    patchOperations.Add(PatchOperation.Remove("/lon"));
                    
                    // Make Timestamp numeric
                    if(!decimal.TryParse(flight.Timestamp, out var Timestamp)) {
                        flight.Timestamp = ((DateTimeOffset)DateTime.Parse(flight.Timestamp)).ToUnixTimeSeconds().ToString();
                    }
                    patchOperations.Add(PatchOperation.Set("/Timestamp", decimal.Parse(flight.Timestamp)));
                    
                    container.PatchItemAsync<Flight>(
                        id: flight.id,
                        partitionKey: new Microsoft.Azure.Cosmos.PartitionKey(flight.flight),
                        patchOperations: patchOperations);
                    }
                } catch(Exception ex) {
                    log.LogError(ex, ex.Message);
                }       
            }
        }
    }
}
