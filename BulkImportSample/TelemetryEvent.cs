using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BulkImportSample
{
    class TelemetryEvent
    {
        public string id { get; set; }

        public string region { get; set; }

        public string postId { get; set; }

        public string userId { get; set; }

        public string trafficSource { get; set; }

        public string device { get; set; }

        public DateTime timestamp { get; set; }

        public string date { get; set; }

        public string UserAgent { get; set; }

        public int sessionLength { get; set; }

        public string contentPreview { get; set; }

        public string partitionKey { get; set; }

        public string day { get; set; }
    }

    class CartOperationEvent
    {
        public string id { get; set; }
        public string CartID { get; set; }
        public string Action { get; set; }
        public string Item { get; set; }
        public double Price { get; set; }
        public string UserName { get; set; }
        public string Country { get; set; }
        public string Address { get; set; }

    }

    class ProductPageTelemetryEvent
    {
        public string id { get; set; }

        public string region { get; set; }

        public string productPageId { get; set; }
        public string productName { get; set; }
        public string price { get; set; }
        public int quantity { get; set; }
        public string action { get; set; }

        public string userId { get; set; }

        public string trafficSource { get; set; }

        public string device { get; set; }

        public DateTime timestamp { get; set; }

        public string date { get; set; }

        public string UserAgent { get; set; }

        public int sessionLength { get; set; }

        public string contentPreview { get; set; }

        public string partitionKey { get; set; }

        public string day { get; set; }
    }

    class IOTTelemetryEvent
    {
        public string id { get; set; }

        public string vin { get; set; }

        public string EventName { get; set; }

        public string Description { get; set; }

        public double s1 { get; set; }

        public double s2 { get; set; }

        public double s3 { get; set; }

        public double s4 { get; set; }

        public double s5 { get; set; }

        public double s6 { get; set; }

        public DateTime timestamp { get; set; }

        public string date { get; set; }

        public string region { get; set; }

        public string partitionKey { get; set; }


    }
}
