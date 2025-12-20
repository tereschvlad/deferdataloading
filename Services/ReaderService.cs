using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using RabbitMQ.Client;

namespace DeferDataLoading;

internal class ReaderService : IReaderService
{
    private readonly ILogger<ReaderService> _logger;
    private readonly IDbReaderService _dbReaderService;
    private readonly IMongoDbWriterService _mongoDbService;
    private readonly ConnectionDataOption _connectionDataOption;

    public ReaderService(ILogger<ReaderService> logger,
        IDbReaderService dbReaderService,
        IMongoDbWriterService mongoDbService, 
        IOptions<ConnectionDataOption> connectionDataOption)
    {
        _logger = logger;
        _dbReaderService = dbReaderService;
        _mongoDbService = mongoDbService;
        _connectionDataOption = connectionDataOption.Value;
    }

    public async Task ReadDataAsync()
    {
        try
        {
            var factory = new ConnectionFactory() 
            { 
                HostName = _connectionDataOption.RabbitMqHostName, 
                Port = _connectionDataOption.RabbitMqPort, 
                Password = _connectionDataOption.RabbitMqPassword, 
                UserName = _connectionDataOption.RabbitMqUserName
            };

            using var connection = await factory.CreateConnectionAsync();
            using var channel = await connection.CreateChannelAsync();

            var message = await channel.BasicGetAsync(_connectionDataOption.QueueName, false);
            while (message != null)
            {
                try
                {
                    var body = message.Body.ToArray();
                    var msg = System.Text.Encoding.UTF8.GetString(body);

                    var requestData = JsonSerializer.Deserialize<RequestDataModel>(msg);

                    _logger.LogInformation("To do request {RequestName} application {Application}", requestData.RequestName, requestData.Application);

                    var rows = await _dbReaderService.ReadDataAsync(requestData.Request, requestData.Parameters);
                    
                    _logger.LogInformation("Get {Count} rows.", rows.Count());

                    IEnumerable<BsonDocument> docs = rows.Select(x =>
                    {
                        var dict = (IDictionary<string, object>)x;

                        var json = JsonSerializer.Serialize(dict);
                        return BsonSerializer.Deserialize<BsonDocument>(json);
                    });

                    var resultRequestData = new ResultRequestDataModel
                    {
                        Request = requestData.Request,
                        Parameters = requestData.Parameters,
                        Rows = docs,
                        CreateDate = DateTime.UtcNow,
                        Application = requestData.Application,
                        UserName = requestData.UserName,
                        RequestName = requestData.RequestName,
                        MongoCollectionName = requestData.MongoCollectionName
                    };

                    await _mongoDbService.WriteDataAsync(resultRequestData);

                    _logger.LogInformation("Writed into mongodb {MongoCollectionName}.", resultRequestData.MongoCollectionName);

                    await channel.BasicAckAsync(message.DeliveryTag, false);
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing message");
                    await channel.BasicNackAsync(message.DeliveryTag, false, false);
                }

                message = await channel.BasicGetAsync(_connectionDataOption.QueueName, false);
            }
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error of reading data");
        }
    }
}
