DECLARE @databaseName NVARCHAR(100) = 'MofidDW';
SELECT TOP (1000)
    [ID],
    [DatabaseName],
    [SchemaName],
    [IndexName],
    [IndexType],
    [PartitionNumber],
    [StartTime],
    [EndTime],
    datediff(minute, StartTime, EndTime) as duration,
    CASE
        WHEN CHARINDEX('rebuild', command) > 0 THEN 'REBUILD'
        WHEN CHARINDEX('reorganize', command) > 0 THEN 'REORGANIZE'
    END AS alterIndexType,
    [Command],
    [ErrorNumber],
    [ErrorMessage]
FROM [master].[dbo].[CommandLog]
WHERE DatabaseName = @databaseName
      AND CommandType = 'ALTER_INDEX'
ORDER BY CAST(ExtendedInfo.value('(/ExtendedInfo/PageCount)[1]', 'INT') AS INT) DESC,
         CAST(ExtendedInfo.value('(/ExtendedInfo/Fragmentation)[1]', 'FLOAT') AS FLOAT) DESC;
