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
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.DataContracts;
using Microsoft.ApplicationInsights.Extensibility;

namespace CloudyDemos.Aircraft
{
    public class LocationOrchestrator
    {
        private static readonly string _databaseId = "Aircraft";
        private static readonly string _flightsContainerId = "flights";

        private readonly TelemetryClient telemetryClient;

        /// Using dependency injection will guarantee that you use the same configuration for telemetry collected automatically and manually.
        public LocationOrchestrator(TelemetryConfiguration telemetryConfiguration)
        {
            this.telemetryClient = new TelemetryClient(telemetryConfiguration);
        }

        public static CosmosClient cosmosClient
        {
            get
            {
                if (_cosmosClient is null)
                    _cosmosClient = new CosmosClient(Environment.GetEnvironmentVariable("cloudyaircraftdata_DOCUMENTDB"));
                return _cosmosClient;
            }
        }
        public static Database database
        {
            get
            {
                if (_database is null)
                    cosmosClient.GetDatabase(_databaseId);
                return _database;
            }
        }
        private static CosmosClient _cosmosClient;
        private static Database _database = cosmosClient.GetDatabase(_databaseId);
        private static Container flightsContainer = database.GetContainer(_flightsContainerId);

        [FunctionName("Location-Orchestrator")]
        public static async Task RunOrchestrator([OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var parallelTasks = new List<Task>();

            Flight[] workBatch = await context.CallActivityAsync<Flight[]>("GetFlightsWithMissingLocation", null);

            for (int i = 0; i < workBatch.Length; i++)
            {
                Task task = context.CallActivityAsync("UpdateFlight", workBatch[i]);
                parallelTasks.Add(task);
            }

            await Task.WhenAll(parallelTasks);

        }

        [FunctionName("GetFlightsWithMissingLocation")]
        public static Flight[] GetFlights([ActivityTrigger] string name, ILogger log)
        {
            // We want to query from the beginning up to docs created no newer than two hours in the past 
            long timeStampTwoHoursAgo = new DateTimeOffset(DateTime.UtcNow.AddHours(-2)).ToUnixTimeSeconds();
            string query = string.Format("select * from c where c.lat > 0 and c._ts < {0}", timeStampTwoHoursAgo);
            log.LogTrace(query);
            List<Flight> Flights = new List<Flight>();

            using (FeedIterator<Flight> feedIterator = flightsContainer.GetItemQueryIterator<Flight>(query))
            {
                while (feedIterator.HasMoreResults)
                {
                    foreach (var item in feedIterator.ReadNextAsync().GetAwaiter().GetResult())
                    {
                        Flights.Add(item);
                    }
                }
            }
            log.LogInformation(string.Format("Retrieved {0} Flights with unprocessed Location data that need to be proccessed.", Flights.Count));
            return Flights.ToArray();
        }

        [FunctionName("UpdateFlight")]
        public void UpdateFlight([ActivityTrigger] Flight flight, ILogger log)
        {
            if (flight.location != null || flight.lat is null || flight.lon is null)
                return;

            var container = _database.GetContainer(_flightsContainerId);

            // Convert lat & lon to a GeoSpatial Point so we can do distance calcs on it (https://docs.microsoft.com/en-us/azure/cosmos-db/sql/sql-query-geospatial-query)
            List<PatchOperation> patchOperations = new List<PatchOperation>();
            Location location = new Location { type = "Point", coordinates = new List<double>() { flight.lon.GetValueOrDefault(), flight.lat.GetValueOrDefault() } };
            patchOperations.Add(PatchOperation.Add("/Location", location));
            patchOperations.Add(PatchOperation.Remove("/lat"));
            patchOperations.Add(PatchOperation.Remove("/lon"));

            // Make Timestamp numeric
            if (!decimal.TryParse(flight.Timestamp, out var Timestamp))
            {
                flight.Timestamp = ((DateTimeOffset)DateTime.Parse(flight.Timestamp)).ToUnixTimeSeconds().ToString();
            }
            patchOperations.Add(PatchOperation.Set("/Timestamp", decimal.Parse(flight.Timestamp)));

            container.PatchItemAsync<Flight>(
                id: flight.id,
                partitionKey: new Microsoft.Azure.Cosmos.PartitionKey(flight.flight),
                patchOperations: patchOperations);

            // Count this flight
            this.telemetryClient.GetMetric(flight.id).TrackValue(1);
        }

        [FunctionName("Location-Orchestrator-Start")]
        public static async Task Start(
            [TimerTrigger("0 0 8 * * *")] TimerInfo timerInfo,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Check if an instance with our ID "Distinct-Flight-Orchestrator" already exists or an existing one stopped running (completed/failed/terminated).
            var existingInstance = await starter.GetStatusAsync("Location-Orchestrator");
            if (existingInstance == null
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Completed
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Failed
            || existingInstance.RuntimeStatus == OrchestrationRuntimeStatus.Terminated)
            {
                // An instance with our ID "Distinct-Flight-Orchestrator" doesn't exist or an existing one stopped running, create one.
                await starter.StartNewAsync("Location-Orchestrator", "Location-Orchestrator", timerInfo);
                log.LogInformation($"Started new instance of Location-Orchestrator.");
            }
            else
            {
                log.LogError($"Failed to started new instance of Location-Orchestrator");
            }
        }
    }
}