namespace CloudyDemos.DataProcessor
{
    using System;
    using System.IO;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;
    using Microsoft.Azure.Devices.Client;
    using Microsoft.Azure.Devices.Client.Transport.Mqtt;
    using Microsoft.Extensions.Configuration;
    using System.Text.Json;
    using System.Net.Http;
    using System.Runtime.Loader;
    using System.Collections.Generic;

    class Program
    {

        static HttpClient client = null;
        static double lastTime = 0;
        static string PiAwareUri = string.Empty;
        static readonly TimeSpan fetchDataDelay = TimeSpan.FromSeconds(1);

        public static void Main()
        {
            Init().Wait();

            // Wait until the app unloads or is cancelled
            var cts = new CancellationTokenSource();
            AssemblyLoadContext.Default.Unloading += (ctx) => cts.Cancel();
            Console.CancelKeyPress += (sender, cpe) => cts.Cancel();
            WhenCancelled(cts.Token).Wait();
        }

        /// <summary>
        /// Handles cleanup operations when app is cancelled or unloads
        /// </summary>
        public static Task WhenCancelled(CancellationToken cancellationToken)
        {
            var tcs = new TaskCompletionSource<bool>();
            cancellationToken.Register(s => ((TaskCompletionSource<bool>)s).SetResult(true), tcs);
            return tcs.Task;
        }

        /// <summary>
        /// Initializes the ModuleClient and sets up the callback to receive
        /// messages containing temperature information
        /// </summary>
        static async Task Init()
        {
            Console.WriteLine("DataProcessor Init() started.");

            IConfiguration configuration = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config/appsettings.json", optional: true)
                .AddEnvironmentVariables()
                .Build();

            TransportType transportType = configuration.GetValue("ClientTransportType", TransportType.Amqp_Tcp_Only);
            PiAwareUri = configuration.GetValue("PiAwareUri", "http://192.168.86.78:8080/data/aircraft.json");

            ModuleClient moduleClient = await ModuleClient.CreateFromEnvironmentAsync(transportType);
            await moduleClient.OpenAsync();
            await SendAircraftData(moduleClient);

            Console.WriteLine("DataProcessor Init() finished - using " + PiAwareUri);
        }

        static async Task SendAircraftData(ModuleClient moduleClient)
        {
            int count = 0;
            using (client = new HttpClient())
            {
                while (true)
                {

                    try
                    {
                        HttpResponseMessage response = await client.GetAsync(PiAwareUri);
                        response.EnsureSuccessStatusCode();
                        Byte[] bytes = await response.Content.ReadAsByteArrayAsync();
                        PiAware piAware = JsonSerializer.Deserialize<PiAware>(bytes);

                        if (lastTime != piAware.now)
                        {
                            using (var eventMessage = new Message(bytes))
                            {
                                eventMessage.Properties.Add("MessageCount", piAware.messages.ToString());
                                eventMessage.Properties.Add("TimeStamp", piAware.now.ToString());
                                eventMessage.Properties.Add("CountOfAircraft", piAware.aircraft.Count.ToString());
                                eventMessage.Properties.Add("SequenceNumber", count.ToString());
                                await moduleClient.SendEventAsync("PiAwareData", eventMessage);
                                // This route needs to be added to get these message to IoT Hub:
                                // "FROM /messages/modules/datafetcher/outputs/PiAwareData INTO $upstream"

                            }
                            count++;
                        }

                        lastTime = piAware.now;
                    }
                    catch (HttpRequestException httpRequestException) {
                        Console.WriteLine(string.Format("Error getting Data in SendAircraftData: {0}.", httpRequestException.Message));
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(string.Format("Error thrown in SendAircraftData: {0}. Stacktrace: {1}", e.Message, e.StackTrace));
                    }
                    finally
                    {
                        await Task.Delay(fetchDataDelay);
                    }
                }
            }
        }
    }

    public class Aircraft
    {
        public string hex { get; set; }
        public int alt_baro { get; set; }
        public int alt_geom { get; set; }
        public double gs { get; set; }
        public double track { get; set; }
        public int baro_rate { get; set; }
        public double nav_qnh { get; set; }
        public int nav_altitude_mcp { get; set; }
        public double nav_heading { get; set; }
        public int version { get; set; }
        public int nic_baro { get; set; }
        public int nac_p { get; set; }
        public int nac_v { get; set; }
        public int sil { get; set; }
        public string sil_type { get; set; }
        public int gva { get; set; }
        public int sda { get; set; }
        // public IList<undefined> mlat { get; set; }
        // public IList<undefined> tisb { get; set; }
        public int messages { get; set; }
        public double seen { get; set; }
        public double rssi { get; set; }

    }
    public class PiAware
    {
        public double now { get; set; }
        public long messages { get; set; }
        public List<Aircraft> aircraft { get; set; }

    }
}
