/*
 A) Stored Procedure – Fato_PrazosExpedicao (SQL Server)
 Objetivo: carregar dbo.Fato_PrazosExpedicao para consumo de BI.

 Regras:
 - Últimos 30 dias de pedidos expedidos
 - PrazoDias = DATEDIFF(day, DataPedido, DataExpedicao)
 - Se houver múltiplas expedições, considerar a primeira
 - Evitar duplicidades na fato (idempotente)
 - Registrar DataCarga (data/hora atual)

 Critérios de aceite:
 - Upsert seguro (sem duplicar), DataCarga registrada
 - Parâmetro opcional @DataRef DATETIME2 = NULL (assumir SYSUTCDATETIME quando NULL)
*/

/* ---------- SETUP MÍNIMO (ajuste livremente) ---------- */
IF OBJECT_ID('dbo.Expedicao', 'U') IS NOT NULL DROP TABLE dbo.Expedicao;
IF OBJECT_ID('dbo.Pedido',    'U') IS NOT NULL DROP TABLE dbo.Pedido;
IF OBJECT_ID('dbo.Fato_PrazosExpedicao', 'U') IS NOT NULL DROP TABLE dbo.Fato_PrazosExpedicao;

CREATE TABLE dbo.Pedido (
    PedidoId    BIGINT        NOT NULL PRIMARY KEY,
    ClienteId   BIGINT        NOT NULL,
    DataPedido  DATETIME2(0)  NOT NULL,
    Status      VARCHAR(20)   NOT NULL
);

CREATE TABLE dbo.Expedicao (
    ExpedicaoId    BIGINT IDENTITY(1,1) PRIMARY KEY,
    PedidoId       BIGINT        NOT NULL,
    DataExpedicao  DATETIME2(0)  NOT NULL,
    CarrierCode    VARCHAR(20)   NULL,
    CONSTRAINT FK_Expedicao_Pedido FOREIGN KEY (PedidoId) REFERENCES dbo.Pedido(PedidoId)
);

CREATE TABLE dbo.Fato_PrazosExpedicao (
    PedidoId               BIGINT        NOT NULL PRIMARY KEY, -- Chave da fato é o PedidoId
    DataPedido             DATETIME2(0)  NOT NULL,
    DataPrimeiraExpedicao  DATETIME2(0)  NOT NULL,
    PrazoDias              INT           NOT NULL,
    DataCarga              DATETIME2(0)  NOT NULL DEFAULT SYSUTCDATETIME()
);

CREATE INDEX IX_Expedicao_Pedido_Data ON dbo.Expedicao(PedidoId, DataExpedicao);

/* ---------- DADOS DE EXEMPLO ---------- */
DECLARE @hoje DATETIME2(0) = SYSUTCDATETIME();

INSERT INTO dbo.Pedido (PedidoId, ClienteId, DataPedido, Status) VALUES
 (101, 1, DATEADD(DAY, -31, @hoje), 'Pago'),    -- pedido > 30d, mas expedicao = 30d (entra)
 (102, 2, DATEADD(DAY, -15, @hoje), 'Pago'),
 (103, 3, DATEADD(DAY, -10, @hoje), 'Pago'),
 (104, 4, DATEADD(DAY,  -5, @hoje), 'Pago'),
 (105, 5, DATEADD(DAY,  -3, @hoje), 'Pago'),
 (106, 6, DATEADD(DAY, -40, @hoje), 'Pago');   -- pedido antigo, expedicao antiga (não entra)

INSERT INTO dbo.Expedicao (PedidoId, DataExpedicao, CarrierCode) VALUES
 (101, DATEADD(DAY, -30, @hoje), 'AA'), -- expedição há 30 dias (na janela)
 (102, DATEADD(DAY, -10, @hoje), 'BB'),
 (102, DATEADD(DAY,  -9, @hoje), 'BB'), -- múltiplas: considerar a primeira (-10)
 (103, DATEADD(DAY,  -8, @hoje), 'CC'),
 (104, DATEADD(DAY,  -4, @hoje), 'DD'),
 (105, DATEADD(DAY,  -1, @hoje), 'EE'),
 (106, DATEADD(DAY, -35, @hoje), 'FF'); -- expedição fora da janela (35 dias)

GO

/* ---------- IMPLEMENTAÇÃO DA PROCEDURE ---------- */
CREATE OR ALTER PROCEDURE dbo.prc_CarregarFatoPrazosExpedicao
    @DataRef DATETIME2(0) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON; -- Garante rollback em caso de erro

    -- Define a data de referência (hoje) e a data de corte (30 dias atrás)
    DECLARE @DataReferencia DATETIME2(0) = COALESCE(@DataRef, SYSUTCDATETIME());
    -- Define o início da janela: exatamente 30 dias antes da data de referência.
    -- O uso de >= garante que expedições no momento exato do corte (ex: -30 dias) sejam incluídas.
    DECLARE @DataCorte DATETIME2(0) = DATEADD(DAY, -30, @DataReferencia);
    DECLARE @DataCargaAtual DATETIME2(0) = SYSUTCDATETIME();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- 1. CTE para buscar a primeira expedição de cada pedido na janela de 30 dias
        ;WITH ExpedicoesRecentes AS (
            SELECT
                e.PedidoId,
                MIN(e.DataExpedicao) AS DataPrimeiraExpedicao
            FROM
                dbo.Expedicao AS e
            WHERE
                e.DataExpedicao >= @DataCorte -- Início da janela (inclusive)
                AND e.DataExpedicao <= @DataReferencia -- Fim da janela (inclusive)
            GROUP BY
                e.PedidoId
        ),
        -- 2. CTE para juntar com dados do pedido e calcular o prazo
        DadosParaFato AS (
            SELECT
                p.PedidoId,
                p.DataPedido,
                er.DataPrimeiraExpedicao,
                DATEDIFF(DAY, p.DataPedido, er.DataPrimeiraExpedicao) AS PrazoDias
            FROM
                ExpedicoesRecentes AS er
            JOIN
                dbo.Pedido AS p ON er.PedidoId = p.PedidoId
        )
        -- 3. Operação de UPSERT (MERGE) para garantir idempotência
        MERGE INTO dbo.Fato_PrazosExpedicao AS Fato
        USING DadosParaFato AS Source
        ON (Fato.PedidoId = Source.PedidoId) -- Chave de negócio (constraint UQ)

        -- Se o PedidoId já existe na fato, atualiza
        WHEN MATCHED AND (
               Fato.DataPedido <> Source.DataPedido
            OR Fato.DataPrimeiraExpedicao <> Source.DataPrimeiraExpedicao
            OR Fato.PrazoDias <> Source.PrazoDias
        ) THEN
            UPDATE SET
                DataPedido = Source.DataPedido,
                DataPrimeiraExpedicao = Source.DataPrimeiraExpedicao,
                PrazoDias = Source.PrazoDias,
                DataCarga = @DataCargaAtual -- Atualiza data de carga na modificação

        -- Se o PedidoId é novo, insere
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                PedidoId,
                DataPedido,
                DataPrimeiraExpedicao,
                PrazoDias,
                DataCarga
            )
            VALUES (
                Source.PedidoId,
                Source.DataPedido,
                Source.DataPrimeiraExpedicao,
                Source.PrazoDias,
                @DataCargaAtual -- Define data de carga na inserção
            );
            -- Cláusula WHEN NOT MATCHED BY SOURCE (ex: DELETE) não é necessária aqui.

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Propaga o erro
        THROW;
    END CATCH;
END
GO

/* ---------- TESTES ---------- */

-- 1. Execução inicial (deve carregar 5 pedidos: 101, 102, 103, 104, 105)
-- Pedido 106 não entra (expedição > 30 dias)
PRINT 'Executando Carga 1 (Inicial)...';
EXEC dbo.prc_CarregarFatoPrazosExpedicao;
SELECT 'Carga 1' AS Teste, * FROM dbo.Fato_PrazosExpedicao ORDER BY PedidoId;
GO

-- 2. Re-execução (não deve duplicar nem alterar nada)
PRINT 'Executando Carga 2 (Idempotência)...';
EXEC dbo.prc_CarregarFatoPrazosExpedicao;
SELECT 'Carga 2' AS Teste, * FROM dbo.Fato_PrazosExpedicao ORDER BY PedidoId;
GO

-- 3. Simular nova expedição para pedido 102 (mais antiga) e mudança no pedido 103
DECLARE @hoje DATETIME2(0) = SYSUTCDATETIME();
UPDATE dbo.Pedido SET DataPedido = DATEADD(DAY, -12, @hoje) WHERE PedidoId = 103; -- Mudou DataPedido
INSERT INTO dbo.Expedicao (PedidoId, DataExpedicao, CarrierCode) 
VALUES (102, DATEADD(DAY, -12, @hoje), 'XX'); -- Expedição mais antiga que a de -10 dias
GO

PRINT 'Executando Carga 3 (Atualização de dados)...';
EXEC dbo.prc_CarregarFatoPrazosExpedicao;
-- Pedido 102 deve atualizar DataPrimeiraExpedicao para -12 dias e recalcular PrazoDias
-- Pedido 103 deve atualizar DataPedido e recalcular PrazoDias
SELECT 'Carga 3' AS Teste, * FROM dbo.Fato_PrazosExpedicao ORDER BY PedidoId;
GO