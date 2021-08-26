//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace BulkImportSample
{
    using System;
    using System.Configuration;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Threading;
    using System.Threading.Tasks;

    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Microsoft.Azure.CosmosDB.BulkExecutor;
    using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;

    class Program
    {
        private static readonly string EndpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
        private static readonly string AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly int CollectionThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);

        public static void Main(string[] args)
        {
            new Program().Go().Wait();
        }

        public async Task Go()
        {

            while (true)
            {
                PrintPrompt();

                var c = Console.ReadKey(true);
                switch (c.Key)
                {
                    case ConsoleKey.D1:
                        await RunBulkScenario("VehicleData");
                        break;
                    case ConsoleKey.D2:
                        await RunBulkScenario("EventsPartitionedByDate");
                        break;
                    case ConsoleKey.D3:
                        await RunBulkScenario("EventsPartitionedByVin_Date");
                        break;
                    case ConsoleKey.Escape:
                        Console.WriteLine("Exiting...");
                        return;
                    default:
                        Console.WriteLine("Invalid choice");
                        break;
                }
            }
        }

        private void PrintPrompt()
        {
            //Console.WriteLine("Summary:");
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine(String.Format("Endpoint: {0}", EndpointUrl));
            //Console.WriteLine(String.Format("Collection : {0}.{1}", DatabaseName, CollectionName));
            Console.WriteLine("--------------------------------------------------------------------- ");
            Console.WriteLine("");
            Console.WriteLine("Press for bulk import:\n");

            Console.WriteLine("1 - Import telemetry data"); //TODO Move this out
            Console.WriteLine("2 - Partition by Date"); //TODO Move this out
            Console.WriteLine("3 - Partition by Vin_Date"); //TODO Move this out
            Console.WriteLine("--------------------------------------------------------------------- "); 
        }

        private async Task RunBulkScenario(string collectionName)
        {
            using (var client = new DocumentClient(
                            new Uri(EndpointUrl),
                            AuthorizationKey,
                            new ConnectionPolicy
                            {
                                ConnectionMode = ConnectionMode.Direct,
                                ConnectionProtocol = Protocol.Tcp
                            }))
            {
                Console.WriteLine("\n");
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine("Starting bulk insert for collection: {0}", collectionName);
                Console.ResetColor();
                try
                {
                    await RunBulkImportAsync(client, collectionName);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Exception caught: {ex.Message}");
                }
            }
        }

        /// <summary>
        /// Driver function for bulk import.
        /// </summary>
        /// <returns></returns>
        private async Task RunBulkImportAsync(DocumentClient client, string collectionName)
        {
            // Cleanup on start if set in config.

            DocumentCollection dataCollection = null;
            try
            {
                if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]))
                {
                    Database database = Utils.GetDatabaseIfExists(client, DatabaseName);
                    if (database != null)
                    {
                        await client.DeleteDatabaseAsync(database.SelfLink);
                    }

                    Trace.TraceInformation("Creating database {0}", DatabaseName);
                    database = await client.CreateDatabaseAsync(new Database { Id = DatabaseName });

                    // Recommend pre-creating collections with desired partition key settings from the Portal before running this demo
                    Trace.TraceInformation(String.Format("Creating collection {0} with {1} RU/s", collectionName, CollectionThroughput));
                    dataCollection = await Utils.CreatePartitionedCollectionAsync(client, DatabaseName, collectionName, CollectionThroughput);
                }
                else
                {
                    dataCollection = Utils.GetCollectionIfExists(client, DatabaseName, collectionName);
                    if (dataCollection == null)
                    {
                        throw new Exception("The data collection does not exist");
                    }
                }
            }
            catch (Exception de)
            {
                Trace.TraceError("Unable to initialize, exception message: {0}", de.Message);
                throw;
            }

            // Prepare for bulk import.

            // Creating documents with simple partition key here.
            string partitionKeyProperty = dataCollection.PartitionKey.Paths[0].Replace("/", "");

            long numberOfDocumentsToGenerate = long.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToImport"]);
            int numberOfBatches = int.Parse(ConfigurationManager.AppSettings["NumberOfBatches"]);
            long numberOfDocumentsPerBatch = (long)Math.Floor(((double)numberOfDocumentsToGenerate) / numberOfBatches);

            // Set retry options high for initialization (default values).
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            IBulkExecutor bulkExecutor = new BulkExecutor(client, dataCollection);
            await bulkExecutor.InitializeAsync();

            // Set retries to 0 to pass control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            BulkImportResponse bulkImportResponse = null;
            long totalNumberOfDocumentsInserted = 0;
            double totalRequestUnitsConsumed = 0;
            double totalTimeTakenSec = 0;

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;


            for (int i = 0; i < numberOfBatches; i++)
            {
                // Generate JSON-serialized documents to import.

                List<string> documentsToImportInBatch = new List<string>();
                long prefix = i * numberOfDocumentsPerBatch;

                Trace.TraceInformation(String.Format("Generating {0} documents to import for batch {1}", numberOfDocumentsPerBatch, i));
                //documentsToImportInBatch = Utils.GenerateRandomDocumentWebsiteTelemetryString((int)numberOfDocumentsPerBatch);
                //documentsToImportInBatch = Utils.GenerateRandomDocumentTelemetryString((int)numberOfDocumentsPerBatch);
                documentsToImportInBatch = Utils.GenerateRandomIOTTelemetryString((int)numberOfDocumentsPerBatch);


                //}
                // Invoke bulk import API.

                var tasks = new List<Task>();

                tasks.Add(Task.Run(async () =>
                {
                    Trace.TraceInformation(String.Format("Executing bulk import for batch {0}", i));
                    do
                    {
                        try
                        {
                            bulkImportResponse = await bulkExecutor.BulkImportAsync(
                                documents: documentsToImportInBatch,
                                enableUpsert: true,
                                disableAutomaticIdGeneration: true,
                                maxConcurrencyPerPartitionKeyRange: null,
                                maxInMemorySortingBatchSize: null,
                                cancellationToken: token);
                        }
                        catch (DocumentClientException de)
                        {
                            Trace.TraceError("Document client exception: {0}", de);
                            break;
                        }
                        catch (Exception e)
                        {
                            Trace.TraceError("Exception: {0}", e);
                            break;
                        }
                    } while (bulkImportResponse.NumberOfDocumentsImported < documentsToImportInBatch.Count);

                    //Trace.WriteLine(String.Format("\nSummary for batch {0}:", i));
                    //Trace.WriteLine("--------------------------------------------------------------------- ");
                    //Trace.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                    //    bulkImportResponse.NumberOfDocumentsImported,
                    //    Math.Round(bulkImportResponse.NumberOfDocumentsImported / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                    //    Math.Round(bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                    //    bulkImportResponse.TotalTimeTaken.TotalSeconds));
                    //Trace.WriteLine(String.Format("Average RU consumption per document: {0}",
                    //    (bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.NumberOfDocumentsImported)));
                    //Trace.WriteLine("---------------------------------------------------------------------\n ");

                    int numberOfRegions = 3;

                    totalNumberOfDocumentsInserted += bulkImportResponse.NumberOfDocumentsImported;
                    totalRequestUnitsConsumed += bulkImportResponse.TotalRequestUnitsConsumed;
                    totalTimeTakenSec += bulkImportResponse.TotalTimeTaken.TotalSeconds;

                    // Code to summarize running total:
                    Console.WriteLine("--------------------------------------------------------------------- ");
                    Console.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                        totalNumberOfDocumentsInserted,
                        Math.Round(totalNumberOfDocumentsInserted / totalTimeTakenSec),
                        Math.Round(totalRequestUnitsConsumed / (numberOfRegions * totalTimeTakenSec)),
                        totalTimeTakenSec));
                    Console.WriteLine(String.Format("Average RU consumption per document: {0}",
                        (totalRequestUnitsConsumed / (numberOfRegions * totalNumberOfDocumentsInserted)))); //divide by number of regions


                //Console.WriteLine(String.Format("Total RU's consumed: {0}",
                //    (totalRequestUnitsConsumed)));
                //Console.WriteLine(String.Format("Total # of Documents inserted: {0}",
                //    (totalNumberOfDocumentsInserted)));

                Console.WriteLine("--------------------------------------------------------------------- ");

                },
                token));


                //tasks.Add(Task.Run(() =>
                //{
                //    char ch = Console.ReadKey(true).KeyChar;
                //    if (ch == 'c' || ch == 'C')
                //    {
                //        tokenSource.Cancel();
                //        Trace.WriteLine("\nTask cancellation requested.");
                //        Console.WriteLine("\nCancelling import.");

                //    }
                //}));


                await Task.WhenAll(tasks);
            }
            Console.ForegroundColor = ConsoleColor.Green;

            Console.WriteLine("\nData Import Completed");
            Console.ResetColor();
            Console.WriteLine("--------------------------------------------------------------------- ");
            //Console.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
            //    totalNumberOfDocumentsInserted,
            //    Math.Round(totalNumberOfDocumentsInserted / totalTimeTakenSec),
            //    Math.Round(totalRequestUnitsConsumed / totalTimeTakenSec),
            //    totalTimeTakenSec));
            //Console.WriteLine(String.Format("Average RU consumption per document: {0}",
            //    (totalRequestUnitsConsumed / totalNumberOfDocumentsInserted)));
            //Console.WriteLine("--------------------------------------------------------------------- ");

            // Cleanup on finish if set in config.

            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnFinish"]))
            {
                Trace.TraceInformation("Deleting Database {0}", DatabaseName);
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName));
            }

            Trace.WriteLine("\nPress any key to exit.");
            //Console.ReadKey();
        }
    }
}
