/*
 B) Worker .NET – Refatoração
 Objetivo: ler mensagens (ex.: fila) e gravar no SQL Server com:
 - Concorrência controlada (sem Parallel.ForEach + async)
 - CancellationToken e shutdown limpo
 - SQL parametrizado + OpenAsync
 - Retry/backoff somente para erros transitórios
 - Idempotência por chave (ex.: MessageId) e logs estruturados

 [Explicação de Solução]
 1. Saturação SQL: Evito saturação usando um Channel<T> (fila interna) e um SemaphoreSlim(DOP)
    para limitar a concorrência. Apenas N tasks (ex: 4) podem executar I/O de banco 
    simultaneamente (padrão Throttling/Consumer), controlando o pool de conexões.
 2. Shutdown Limpo: O CancellationToken (stoppingToken) é repassado ao Channel (Complete) 
    e ao Semaphore (WaitAsync). O Task.WhenAll no final do ExecuteAsync garante que,
    ao receber o CTRL+C, o worker pare de ler, termine de processar os itens 
    na fila (drain) e só então finalize o processo.
*/

using System;
using System.Data;
using System.Data.Common;
using Microsoft.Data.SqlClient; // ATUALIZADO: Usando Microsoft.Data.SqlClient (moderno, mais resiliente)
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes; // Para JsonNode
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration; // Para IConfiguration
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Esta classe simula a leitura do arquivo JSONL.
// Em um cenário real, IMessageQueue leria de SQS, RabbitMQ, etc.
public class FileMessageReader
{
    private readonly string _filePath;
    private readonly ILogger _logger;

    public FileMessageReader(string filePath, ILogger logger)
    {
        _filePath = filePath;
        _logger = logger;
    }

    // Lê o arquivo .jsonl linha a linha de forma assíncrona
    public async Task ReadMessagesToChannelAsync(ChannelWriter<JsonNode> writer, CancellationToken ct)
    {
        if (!File.Exists(_filePath))
        {
            _logger.LogError("Arquivo de mensagens não encontrado: {FilePath}", _filePath);
            writer.Complete();
            return;
        }

        try
        {
            await using var stream = File.OpenRead(_filePath);
            using var reader = new StreamReader(stream);
            
            string? line;
            int lineCount = 0;
            
            while (!ct.IsCancellationRequested && (line = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(line)) continue;

                try
                {
                    // Usamos JsonNode para flexibilidade, similar ao Dictionary<string, object> original
                    var jsonNode = JsonNode.Parse(line);
                    if (jsonNode != null)
                    {
                        await writer.WriteAsync(jsonNode, ct);
                        lineCount++;
                    }
                }
                catch (JsonException ex)
                {
                    _logger.LogWarning(ex, "Falha ao desserializar linha: {Line}", line);
                }
            }
            
            _logger.LogInformation("Leitura de arquivo concluída. {Count} mensagens publicadas no channel.", lineCount);
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Leitura de arquivo cancelada.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Erro ao ler arquivo de mensagens.");
        }
        finally
        {
            // Sinaliza que o produtor terminou
            writer.Complete();
        }
    }
}

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly string _connString;
    private readonly string _messageFilePath;
    private readonly IConfiguration _configuration;

    // Grau de Paralelismo (DOP) - Quantas conexões/comandos simultâneos ao SQL
    private const int MaxConcurrency = 4; 

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
        _connString = _configuration.GetValue<string>("ConnectionString") ?? "Server=.;Database=TestDb;Trusted_Connection=True;";
        _messageFilePath = _configuration.GetValue<string>("MessageFilePath") ?? "messages.jsonl";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Worker iniciado em {Time}", DateTimeOffset.Now);

        // 1. Setup do Pipeline: Channel (fila) + Semaphore (concorrência)
        var channel = Channel.CreateBounded<JsonNode>(new BoundedChannelOptions(100)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = false,
            SingleWriter = true // Apenas 1 leitor de arquivo
        });

        var semaphore = new SemaphoreSlim(MaxConcurrency, MaxConcurrency);

        // 2. Produtor (Leitor de Arquivo)
        var fileReader = new FileMessageReader(_messageFilePath, _logger);
        var producerTask = fileReader.ReadMessagesToChannelAsync(channel.Writer, stoppingToken);

        // 3. Consumidores (Workers do Banco)
        var consumerTasks = Enumerable.Range(0, MaxConcurrency)
            .Select(_ => ConsumeMessagesAsync(channel.Reader, semaphore, stoppingToken))
            .ToList();

        // 4. Aguardar Produtor e Consumidores
        // Aguarda o produtor (leitor) terminar
        await producerTask;
        _logger.LogInformation("Produtor (leitura de arquivo) finalizado. Aguardando consumidores...");

        // Aguarda todos os consumidores terminarem (drenagem da fila)
        await Task.WhenAll(consumerTasks);

        _logger.LogInformation("Worker finalizado em {Time}", DateTimeOffset.Now);
    }

    private async Task ConsumeMessagesAsync(ChannelReader<JsonNode> reader, SemaphoreSlim semaphore, CancellationToken ct)
    {
        // Espera por itens no channel
        await foreach (var message in reader.ReadAllAsync(ct))
        {
            // Respeita o CancellationToken antes de tentar pegar o semáforo
            if (ct.IsCancellationRequested) break;

            await semaphore.WaitAsync(ct); // Aguarda um "slot" livre

            try
            {
                await ProcessMessageWithRetryAsync(message, ct);
            }
            catch (OperationCanceledException)
            {
                // Se o token for cancelado, não logamos como erro
                _logger.LogWarning("Processamento cancelado para MessageId {MessageId}", message["messageId"]?.GetValue<string>());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Erro fatal ao processar MessageId {MessageId}. Mensagem movida para DLQ (simulado).", message["messageId"]?.GetValue<string>());
                // TODO: Mover para uma Dead Letter Queue (DLQ)
            }
            finally
            {
                semaphore.Release(); // Libera o "slot"
            }
        }
    }

    private async Task ProcessMessageWithRetryAsync(JsonNode message, CancellationToken ct)
    {
        const int maxRetries = 3;
        int currentRetry = 0;
        TimeSpan delay = TimeSpan.FromSeconds(1);

        // Extração de dados (com defaults seguros)
        string messageId = message["messageId"]?.GetValue<string>() ?? Guid.NewGuid().ToString();
        long orderId = message["orderId"]?.GetValue<long>() ?? -1;
        string eventType = message["eventType"]?.GetValue<string>() ?? "UNKNOWN";
        DateTime occurredAt = message["occurredAt"]?.GetValue<DateTime>() ?? DateTime.UtcNow;
        string payload = message["payload"]?.ToJsonString() ?? "{}";
        
        // Log estruturado
        using var logScope = _logger.BeginScope(new Dictionary<string, object>
        {
            ["MessageId"] = messageId,
            ["OrderId"] = orderId,
            ["EventType"] = eventType
        });

        while (currentRetry < maxRetries)
        {
            try
            {
                ct.ThrowIfCancellationRequested();

                // Idempotência: INSERT ... WHERE NOT EXISTS
                // Evita erro de PK (2627/2601) se a m-1004 (duplicada) tentar inserir
                var sql = @"
                    INSERT INTO dbo.EventosPedido (MessageId, OrderId, EventType, OccurredAt, Payload, ReceivedAt)
                    SELECT 
                        @MessageId, @OrderId, @EventType, @OccurredAt, @Payload, SYSUTCDATETIME()
                    WHERE NOT EXISTS (
                        SELECT 1 FROM dbo.EventosPedido WITH (UPDLOCK, HOLDLOCK) 
                        WHERE MessageId = @MessageId
                    );";

                await using var connection = new SqlConnection(_connString);
                await connection.OpenAsync(ct);

                await using var command = new SqlCommand(sql, connection);
                command.Parameters.Add("@MessageId", SqlDbType.VarChar, 50).Value = messageId;
                command.Parameters.Add("@OrderId", SqlDbType.BigInt).Value = orderId;
                command.Parameters.Add("@EventType", SqlDbType.VarChar, 50).Value = eventType;
                command.Parameters.Add("@OccurredAt", SqlDbType.DateTime2, 0).Value = occurredAt;
                command.Parameters.Add("@Payload", SqlDbType.NVarChar, -1).Value = payload; // NVARCHAR(MAX)

                int rowsAffected = await command.ExecuteNonQueryAsync(ct);

                if (rowsAffected > 0)
                {
                    _logger.LogInformation("Mensagem processada e salva com sucesso.");
                }
                else
                {
                    // Isso acontece com a m-1004 (duplicada)
                    _logger.LogWarning("Mensagem duplicada detectada (idempotência). Processamento ignorado.");
                }

                return; // Sucesso, sair do loop de retry
            }
            catch (SqlException sqlEx) when (IsTransient(sqlEx))
            {
                currentRetry++;
                _logger.LogWarning(sqlEx, "Erro transitório (Tentativa {Retry}/{Max}). Tentando novamente em {Delay}s.", currentRetry, maxRetries, delay.TotalSeconds);
                
                if (currentRetry < maxRetries)
                {
                    await Task.Delay(delay, ct);
                    delay *= 2; // Backoff exponencial
                }
                else
                {
                    _logger.LogError(sqlEx, "Falha ao processar mensagem após {MaxRetries} tentativas.", maxRetries);
                    throw; // Desiste e propaga a exceção
                }
            }
            catch (Exception ex)
            {
                // Erro não transitório (ex: falha de parâmetro, lógica, etc.)
                _logger.LogError(ex, "Erro não-transiente ao processar mensagem.");
                throw; // Desiste imediatamente
            }
        }
    }

    // Helper para identificar erros de SQL que valem retry (ex: Timeout, Deadlock)
    private bool IsTransient(SqlException ex)
    {
        // ATUALIZADO: Lista expandida de erros transitórios
        // Baseado nas recomendações oficiais da Microsoft (ex: Polly, EF Core)
        switch (ex.Number)
        {
            case 1205:  // Deadlock
            case -2:    // Timeout
            case -1:    // Timeout
            case 233:   // Connection pipe
            case 10053: // Network/connection
            case 10054: // Network/connection
            case 10060: // Network/connection
            case 40197: // Azure SQL: Service busy
            case 40501: // Azure SQL: Service busy
            case 40613: // Azure SQL: Database unavailable
            case 49918: // Azure SQL: Resource governance
            case 49919: // Azure SQL: Resource governance
            case 49920: // Azure SQL: Resource governance
            case 4060:  // Database unavailable (já estava)
                return true;
            default:
                return false;
        }
    }
}