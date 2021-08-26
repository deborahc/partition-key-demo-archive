//------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//------------------------------------------------------------

namespace BulkImportSample
{
    using Bogus;
    using Microsoft.Azure.Documents;
    using Microsoft.Azure.Documents.Client;
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Configuration;
    using System.Linq;
    using System.Threading.Tasks;

    class Utils
    {
        /// <summary>
        /// Get the collection if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested collection.</returns>
        static internal DocumentCollection GetCollectionIfExists(DocumentClient client, string databaseName, string collectionName)
        {
            if (GetDatabaseIfExists(client, databaseName) == null)
            {
                return null;
            }

            return client.CreateDocumentCollectionQuery(UriFactory.CreateDatabaseUri(databaseName))
                .Where(c => c.Id == collectionName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Get the database if it exists, null if it doesn't.
        /// </summary>
        /// <returns>The requested database.</returns>
        static internal Microsoft.Azure.Documents.Database GetDatabaseIfExists(DocumentClient client, string databaseName)
        {
            return client.CreateDatabaseQuery().Where(d => d.Id == databaseName).AsEnumerable().FirstOrDefault();
        }

        /// <summary>
        /// Create a partitioned collection.
        /// </summary>
        /// <returns>The created collection.</returns>
        static internal async Task<DocumentCollection> CreatePartitionedCollectionAsync(DocumentClient client, string databaseName,
            string collectionName, int collectionThroughput)
        {
            PartitionKeyDefinition partitionKey = new PartitionKeyDefinition
            {
                Paths = new Collection<string> { ConfigurationManager.AppSettings["CollectionPartitionKey"] }
            };
            DocumentCollection collection = new DocumentCollection { Id = collectionName, PartitionKey = partitionKey };

            try
            {
                collection = await client.CreateDocumentCollectionAsync(
                    UriFactory.CreateDatabaseUri(databaseName),
                    collection,
                    new RequestOptions { OfferThroughput = collectionThroughput });
            }
            catch (Exception e)
            {
                throw e;
            }

            return collection;
        }

        static internal String GenerateRandomDocumentString(String id, String partitionKeyProperty, String parititonKeyValue)
        {
            var eventTypes = new string[] { "Harsh_break", "Airbag_deploy", "Check_engine_light" };
            string eventName = eventTypes[int.Parse(parititonKeyValue) % 3];
            return "{\n" +
                "    \"id\": \"" + id + "\",\n" +
                "    \"" + partitionKeyProperty + "\": \"" + parititonKeyValue + "\",\n" +
                "    \"EventName\": \"" + eventName + "\",\n" +
                "    \"Description\": \"\",\n" +
                "    \"s1\": \"38442291.3\",\n" +
                "    \"s2\": \"23959381.2\",\n" +
                "    \"s3\": \"148\",\n" +
                "    \"s4\": \"323\",\n" +
                "    \"s5\": \"32395.9\",\n" +
                "    \"s6\": \"8732\"" +
                "}";
        }

        static internal List<string> GenerateRandomDocumentWebsiteTelemetryString(int numberOfDocumentsPerBatch)
        {
            var trafficSources = new[] { "facebook", "twitter", "search", "wechat", "email" };
            var devices = new[] { "mobile", "desktop", "tablet" };

            // have only 10k posts
            var faker = new Faker();

            var productPageIds = Enumerable.Range(1, 1000)
                  .Select(_ => Guid.NewGuid().ToString())
                  .ToList();

            var cartEvents = new[] { "view", "addToCart", "purchase" };

            var telemetryEvent = new Faker<ProductPageTelemetryEvent>()

                //Ensure all properties have rules. By default, StrictMode is false
                //Set a global policy by using Faker.DefaultStrictMode
                .StrictMode(true)

                //Generate telemetry event
                .RuleFor(o => o.region, f => f.Address.Country())
                .RuleFor(o => o.id, f => Guid.NewGuid().ToString())
                .RuleFor(o => o.productPageId, f => f.PickRandom(productPageIds))
                .RuleFor(o => o.productName, f => f.Commerce.ProductName())
                .RuleFor(o => o.price, f => f.Commerce.Price())
                .RuleFor(o => o.quantity, f => f.Random.Number(1,10))
                .RuleFor(o => o.action, f => f.PickRandom(cartEvents))

                //.RuleFor(o => o.postId, f => Guid.NewGuid().ToString())

                .RuleFor(o => o.userId, f => f.Internet.UserName())
                .RuleFor(o => o.trafficSource, f => f.PickRandom(trafficSources))
                .RuleFor(o => o.UserAgent, f => f.Internet.UserAgent())
                .RuleFor(o => o.device, f => f.PickRandom(devices))
                //.RuleFor(o => o.timestamp, f => f.Date.Between(DateTime.Now, DateTime.Now.AddDays(-1))) //3 days ago
                .RuleFor(o => o.timestamp, f => DateTime.Now) // just today's date
                .RuleFor(o => o.date, (f, m) => $"{m.timestamp.ToString("yyyy-MM-dd")}")
                .RuleFor(o => o.partitionKey, (f, m) => $"{m.productPageId}_{m.date}") // partitionKey is postId_timestamp
                .RuleFor(o => o.sessionLength, f => f.Random.Int(1, 1800))
                .RuleFor(o => o.contentPreview, f => f.Lorem.Sentences())
                .RuleFor(o => o.day, f => DateTime.Now.Date.ToString());



            var events = telemetryEvent.Generate(numberOfDocumentsPerBatch);
            List<string> telemetryEvents = new List<string>();

            foreach (var telEvent in events)
            {
                telemetryEvents.Add(JsonConvert.SerializeObject(telEvent));
            }
            return telemetryEvents;

            // Need a way to run this based on 
        }

        static internal List<string> GenerateRandomDocumentTelemetryString(int numberOfDocumentsPerBatch)
        {
            var trafficSources = new[] { "facebook", "twitter", "search", "wechat", "email" };
            var devices = new[] { "mobile", "desktop", "tablet" };

            // have only 10k posts
            var faker = new Faker();

            //var postsList = Enumerable.Range(1, 100)
            //      .Select(_ => Guid.NewGuid().ToString())
            //      .ToList();

            var telemetryEvent = new Faker<TelemetryEvent>()

                //Ensure all properties have rules. By default, StrictMode is false
                //Set a global policy by using Faker.DefaultStrictMode
                .StrictMode(true)

                //Generate telemetry event
                .RuleFor(o => o.region, f => f.Address.Country())
                .RuleFor(o => o.id, f => Guid.NewGuid().ToString())
                //.RuleFor(o => o.postId, f => f.PickRandom(postsList))
                .RuleFor(o => o.postId, f => Guid.NewGuid().ToString())

                .RuleFor(o => o.userId, f => f.Internet.UserName())
                .RuleFor(o => o.trafficSource, f => f.PickRandom(trafficSources))
                .RuleFor(o => o.UserAgent, f => f.Internet.UserAgent())
                .RuleFor(o => o.device, f => f.PickRandom(devices))
                //.RuleFor(o => o.timestamp, f => f.Date.Between(DateTime.Now, DateTime.Now.AddDays(-1))) //3 days ago
                .RuleFor(o => o.timestamp, f => DateTime.Now) // just today's date
                .RuleFor(o => o.date, (f, m) => $"{m.timestamp.ToString("yyyy-MM-dd")}")
                .RuleFor(o => o.partitionKey, (f, m) => $"{m.postId}_{m.date}") // partitionKey is postId_timestamp
                .RuleFor(o => o.sessionLength, f => f.Random.Int(1, 1800))
                .RuleFor(o => o.contentPreview, f => f.Lorem.Sentences())
                .RuleFor(o => o.day, f => DateTime.Now.Date.ToString());



            var events = telemetryEvent.Generate(numberOfDocumentsPerBatch);
            List<string> telemetryEvents = new List<string>();

            foreach (var telEvent in events) {
                telemetryEvents.Add(JsonConvert.SerializeObject(telEvent));
            }
            return telemetryEvents;

            // Need a way to run this based on 
        }

        static internal List<string> GenerateRandomIOTTelemetryString(int numberOfDocumentsPerBatch)
        {
            var eventTypes = new string[] { "Harsh_break", "Airbag_deploy", "Check_engine_light" };

            // have only 10k posts
            var faker = new Faker();

            //var vinList = Enumerable.Range(1, 50000)
            //      .Select(_ => _.ToString())
            //      .ToList();

            var telemetryEvent = new Faker<IOTTelemetryEvent>()

                //Ensure all properties have rules. By default, StrictMode is false
                //Set a global policy by using Faker.DefaultStrictMode
                .StrictMode(true)

                //Generate telemetry event
                .RuleFor(o => o.id, f => Guid.NewGuid().ToString())
                .RuleFor(o => o.vin, f => f.Vehicle.Vin().ToString())
                .RuleFor(o => o.EventName, f => f.PickRandom(eventTypes))
                .RuleFor(o => o.Description, f => f.Vehicle.Type())
                //.RuleFor(o => o.timestamp, f => f.Date.Between(DateTime.Now, DateTime.Now.AddDays(-1))) //3 days ago
                .RuleFor(o => o.timestamp, f => DateTime.Now) // just today's date
                .RuleFor(o => o.date, (f, m) => $"{m.timestamp.ToString("yyyy-MM-dd")}")
                .RuleFor(o => o.s1, f => f.Random.Double(1, 2000))
                .RuleFor(o => o.s2, f => f.Random.Double(1, 2000))
                .RuleFor(o => o.s3, f => f.Random.Double(1, 2000))
                .RuleFor(o => o.s4, f => f.Random.Double(1, 2000))
                .RuleFor(o => o.s5, f => f.Random.Double(1, 2000))
                .RuleFor(o => o.s6, f => f.Random.Double(1, 2000))
                .RuleFor(o => o.region, f => f.Address.Country())
                .RuleFor(o => o.partitionKey, (f, m) => $"{m.vin}_{m.date}"); // partitionKey is vin_date




            var events = telemetryEvent.Generate(numberOfDocumentsPerBatch);
            List<string> telemetryEvents = new List<string>();

            foreach (var telEvent in events)
            {
                telemetryEvents.Add(JsonConvert.SerializeObject(telEvent));
            }
            return telemetryEvents;

            // Need a way to run this based on 
        }
    }
}