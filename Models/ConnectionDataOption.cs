namespace DeferDataLoading;

internal class ConnectionDataOption
{
    public string DbName { get; init; }

    public string DbConnection { get; init; }

    public string MongoDbConnection { get; init; }

    public string MongoDbName { get; init; }

    public string QueueName { get; init; }

    public string RabbitMqHostName { get; init; }

    public int RabbitMqPort { get; init; }

    public string RabbitMqUserName { get; init; }

    public string RabbitMqPassword { get; init; }

    public string SeqKey { get; init; }

    public string SeqHost { get; init; }

    public int WorkerDelayed { get; init; }

}