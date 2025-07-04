using Confluent.Kafka;
using MediumKafkaBroker.Models; 
using Newtonsoft.Json;
using Npgsql;

// Read configuration from Environment Variables, with fallbacks for local debugging
var connectionString = Environment.GetEnvironmentVariable("POSTGRES_CONNECTION_STRING");

var bootstrapServers = Environment.GetEnvironmentVariable("KAFKA_BOOTSTRAP_SERVERS");

const string KafkaTopic = "mongo.productionData.users";

var consumerConfig = new ConsumerConfig
{
    GroupId = "user-consumer-group",
    BootstrapServers = bootstrapServers,
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

// ... the rest of your code remains exactly the same ...
// (UpsertQuery, DeleteQuery, main loop, etc.)

const string UpsertQuery = """
                           
                               INSERT INTO users (id, name, email, age)
                               VALUES (@id, @name, @email, @age)
                               ON CONFLICT (id) DO UPDATE SET 
                                   name = EXCLUDED.name,
                                   email = EXCLUDED.email,
                                   age = EXCLUDED.age;

                           """;

const string DeleteQuery = @"DELETE FROM users WHERE id = @id;";

await using var conn = new NpgsqlConnection(connectionString);
await conn.OpenAsync();

using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
consumer.Subscribe(KafkaTopic);

Console.WriteLine($"Listening for Kafka messages on topic '{KafkaTopic}' using bootstrap servers '{bootstrapServers}'...");

while (true)
{
    try
    {
        var consumeResult = consumer.Consume(TimeSpan.FromSeconds(2));
        if (consumeResult == null)
        {
            // This is normal, just continue waiting
            continue;
        }

        Console.WriteLine($"Consumed message at offset {consumeResult.Offset}");

        var envelope = JsonConvert.DeserializeObject<DebeziumEnvelope>(consumeResult.Message.Value);
        if (envelope?.Payload == null)
        {
            Console.WriteLine("Envelope or Payload is null, skipping...");
            continue;
        }

        await using var transaction = await conn.BeginTransactionAsync();

        switch (envelope.Payload.Op)
        {
            case "c": // create
            case "u": // update
            case "r": // read (for initial snapshots)
                var userForUpsert = JsonConvert.DeserializeObject<User>(envelope.Payload.After);
                if (userForUpsert is not null)
                {
                    await UpsertUserAsync(conn, userForUpsert);
                }
                break;

            case "d": // delete
                var userForDelete = JsonConvert.DeserializeObject<User>(envelope.Payload.Before);
                if (userForDelete is not null)
                {
                    await DeleteUserAsync(conn, userForDelete);
                }
                break;

            default:
                Console.WriteLine($"Unhandled operation type: {envelope.Payload.Op}");
                break;
        }

        consumer.Commit(consumeResult);
        await transaction.CommitAsync();
    }
    catch (ConsumeException ce)
    {
        Console.WriteLine($"Error consuming Kafka message: {ce.Error.Reason}");
    }
    catch (PostgresException pe)
    {
        Console.WriteLine($"Database operation failed: {pe.MessageText}");
    }
    catch (Exception e)
    {
        Console.WriteLine($"Unhandled exception: {e.Message}");
    }
}
 
static async Task UpsertUserAsync(NpgsqlConnection connection, User user)
{
    await using var cmd = new NpgsqlCommand(UpsertQuery, connection);
    cmd.Parameters.AddWithValue("id", user.Id.Oid);
    cmd.Parameters.AddWithValue("name", user.Name ?? (object)DBNull.Value);
    cmd.Parameters.AddWithValue("email", user.Email ?? (object)DBNull.Value);
    cmd.Parameters.AddWithValue("age", user.Age);

    await cmd.ExecuteNonQueryAsync();
    Console.WriteLine($"Upserted user: {user.Name} (ID: {user.Id.Oid})");
}

static async Task DeleteUserAsync(NpgsqlConnection connection, User user)
{
    await using var cmd = new NpgsqlCommand(DeleteQuery, connection);
    cmd.Parameters.AddWithValue("id", user.Id.Oid);

    var rowsDeleted = await cmd.ExecuteNonQueryAsync();
    Console.WriteLine(rowsDeleted > 0
        ? $"Deleted user: {user.Id.Oid}"
        : $"No user found to delete with ID: {user.Id.Oid}");
}
