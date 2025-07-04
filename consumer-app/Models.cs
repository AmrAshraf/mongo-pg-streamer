using Newtonsoft.Json;

namespace MediumKafkaBroker.Models
{
    // Represents the User object from MongoDB
    public class User
    {   
        [JsonProperty("_id")]
        public MongoId Id { get; set; }
        public string Name { get; set; }
        public string Email { get; set; }
        public int Age { get; set; }
    }

    public class MongoId
    {
        [JsonProperty("$oid")]
        public string Oid { get; set; }
    }

    // Represents the Debezium message envelope
    public class DebeziumEnvelope
    {
        public DebeziumPayload Payload { get; set; }
    }

    public class DebeziumPayload
    {
        public string Op { get; set; } // Operation: c=create, u=update, d=delete
        
        [JsonProperty("after")]
        public string After { get; set; } // JSON string of the document state after the change

        [JsonProperty("before")]
        public string Before { get; set; } // JSON string of the document state before the change
    }
}
