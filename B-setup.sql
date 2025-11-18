-- Tabela de destino para o worker
IF OBJECT_ID('dbo.EventosPedido', 'U') IS NULL
BEGIN
  CREATE TABLE dbo.EventosPedido(
    Id            BIGINT IDENTITY(1,1) PRIMARY KEY,
    MessageId     VARCHAR(50)  NOT NULL,
    OrderId       BIGINT       NOT NULL,
    EventType     VARCHAR(50)  NOT NULL,
    OccurredAt    DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    Payload       NVARCHAR(MAX) NULL,
    ReceivedAt    DATETIME2(0) NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT UQ_EventosPedido_Message UNIQUE (MessageId)
  );
END;
