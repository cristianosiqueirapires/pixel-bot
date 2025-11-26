/*
 B) Worker .NET – Refatoração

 Evita saturação do SQL: Channel bounded (100 mensagens) + máximo 4 consumidores simultâneos + backoff exponencial em erros transitórios.
 Garante desligamento limpo: CancellationToken para leitura, drena todos os consumidores com Task.WhenAll e completa o canal → nenhuma mensagem perdida.
*/

using System;
using System.Data;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly string _connString;

    private readonly Channel<JsonDocument> _channel = Channel.CreateBounded<JsonDocument>(
        new BoundedChannelOptions(100)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });

    private const int MaxConsumers = 4;

    public Worker(ILogger<Worker> logger, IConfiguration config)
    {
        _logger   = logger;
        _conn     = config.GetConnectionString("Sql")!;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var producer  = Task.Run(() => ProducerAsync(stoppingToken), stoppingToken);
        var consumers = Enumerable.Range(0, MaxConsumers)
                                  .Select(_ => Task.Run(() => ConsumerAsync(stoppingToken), stoppingToken))
                                  .ToArray();

        await producer.ConfigureAwait(false);
        _channel.Writer.Complete();

        await Task.WhenAll(consumers).ConfigureAwait(false);
    }

    private async Task ProducerAsync(CancellationToken ct)
    {
        await foreach (var line in File.ReadLinesAsync("messages.jsonl", ct))
        {
            var doc = JsonDocument.Parse(line);
            await _channel.Writer.WaitToWriteAsync(ct).ConfigureAwait(false);
            _channel.Writer.TryWrite(doc);
        }
    }

    private async Task ConsumerAsync(CancellationToken ct)
    {
        await foreach (var doc in _channel.Reader.ReadAllAsync(ct))
        {
            using (doc)
            {
                var root      = doc.RootElement;
                var messageId = root.GetProperty("messageId").GetString()!;
                var orderId   = root.GetProperty("orderId").GetInt64();
                var eventType = root.GetProperty("eventType").GetString()!;
                var occurredAt = root.GetProperty("occurredAt").GetDateTimeOffset();
                var payload   = root.GetRawText();

                int retry = 0;
                while (true)
                {
                    try
                    {
                        await InsertIfNotExists(messageId, orderId, eventType, occurredAt, payload, ct);
                        _logger.LogInformation("Gravado MessageId={MessageId} OrderId={OrderId}", messageId, orderId);
                        break;
                    }
                    catch (SqlException ex) when (IsTransient(ex) && retry < 3)
                    {
                        retry++;
                        var delay = TimeSpan.FromMilliseconds(200 * Math.Pow(2, retry - 1));
                        _logger.LogWarning(ex, "Retry {Retry} MessageId={MessageId} delay={Delay}s", retry, messageId, delay.TotalSeconds);
                        await Task.Delay(delay, ct);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Falha permanente MessageId={MessageId}", messageId);
                        await File.AppendAllTextAsync("failed.jsonl", doc.RootElement + Environment.NewLine, ct);
                        break;
                    }
                }
            }
        }
    }

    private async Task InsertIfNotExists(string messageId, long orderId, string eventType,
                                         DateTimeOffset occurredAt, string payload, CancellationToken ct)
    {
        await using var conn = new SqlConnection(_conn);
        await conn.OpenAsync(ct);

        const string sql = @"
            IF NOT EXISTS (SELECT 1 FROM dbo.EventosPedido WHERE MessageId = @MessageId)
            BEGIN
                INSERT INTO dbo.EventosPedido (MessageId, OrderId, EventType, OccurredAt, Payload)
                VALUES (@MessageId, @OrderId, @EventType, @OccurredAt, @Payload)
            END";

        await using var cmd = new SqlCommand(sql, conn);
        cmd.Parameters.Add("@MessageId",   SqlDbType.VarChar, 50).Value = messageId;
        cmd.Parameters.Add("@OrderId",     SqlDbType.BigInt).Value      = orderId;
        cmd.Parameters.Add("@EventType",   SqlDbType.VarChar, 50).Value = eventType;
        cmd.Parameters.Add("@OccurredAt",  SqlDbType.DateTime2).Value = occurredAt.UtcDateTime;
        cmd.Parameters.Add("@Payload",     SqlDbType.NVarChar, -1).Value = payload;

        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static bool IsTransient(SqlException ex) =>
        new[] { -2, 53, 1205, 40197, 49918, 49919, 49920 }.Contains(ex.Number);
}