SET QUOTED_IDENTIFIER ON
GO
SET ANSI_NULLS ON
GO
DECLARE @LastEventTime AS DATETIME2
DECLARE @FileDirectory NVARCHAR(512)
SELECT TOP 1
    @FileDirectory = LEFT(physical_name, CHARINDEX(':', physical_name)) + '\ExtendedEvent_PerformanceTuning_Mofid\'
FROM sys.master_files
WHERE database_id = 39
DECLARE @FileLocation AS NVARCHAR(max) = @FileDirectory + 'Tuning_Queries*.xel'
SELECT @LastEventTime = MAX([Event Time])
FROM tpcg_waitstats.dbo.ExtendedEvent_PerformanceTuning_Mofid
WHERE ExtendedEventTypeId = 1
DROP TABLE IF EXISTS #EventData
SELECT try_cast(event_data as xml) event_data
     , file_name
     , file_offset
     , DATEADD(MINUTE, DATEDIFF(MINUTE, SYSUTCDATETIME(), SYSDATETIME()), (CAST(timestamp_utc AS DATETIME))) timestamp_utc
INTO #EventData
FROM sys.fn_xe_file_target_read_file(@FileLocation, NULL, NULL, NULL)
WHERE DATEADD(MINUTE, DATEDIFF(MINUTE, SYSUTCDATETIME(), SYSDATETIME()), (CAST(timestamp_utc AS DATETIME))) >= ISNULL(@LastEventTime, DATEADD(hour, -1, GETDATE()))

IF NOT EXISTS (SELECT TOP 1 * FROM #EventData)
BEGIN
    RAISERROR(N'هیچ رکودری لاگ نشده است.', 16, 1);
    RETURN
END

DROP TABLE IF EXISTS #Queries;
WITH EventInfo (event_data, [Event], [Event Time], [SessionId], [ObjectId], [ObjectName], [DBId], [DBName], [Statement]
              , [SQL], [User Name], [Client], [App], [CPU Time], [Duration], [Logical Reads], [Physical Reads]
              , [Writes], [Query Hash], [Plan Hash], [PlanHandle], File_Name, [Stmt Offset], [Stmt Offset End]
               )
AS (SELECT event_data
         , event_data.value('/event[1]/@name', 'SYSNAME')                                                               AS [Event]
         , timestamp_utc                                                                                                AS [Event Time]
         , event_data.value('((/event[1]/action[@name="session_id"]/value/text())[1])', 'INT')                          AS [Session Id]
         , event_data.value('((/event[1]/data[@name="object_id"]/value/text())[1])', 'INT')                             AS [Object Id]
         , event_data.value('((/event[1]/data[@name="object_name"]/value/text())[1])', 'Nvarchar(max)')                 AS [Object Name]
         , event_data.value('((/event[1]/action[@name="database_id"]/value/text())[1])', 'INT')                         AS [DBId]
         , event_data.value('((/event[1]/action[@name="database_name"]/value/text())[1])', 'NVARCHAR(100)')             AS [DBName]
         , event_data.value('((/event[1]/data[@name="statement"]/value/text())[1])', 'NVARCHAR(MAX)')                   AS [Statement]
         , event_data.value('((/event[1]/action[@name="sql_text"]/value/text())[1])', 'NVARCHAR(MAX)')                  AS [SQL]
         , event_data.value('((/event[1]/action[@name="username"]/value/text())[1])', 'NVARCHAR(255)')                  AS [User Name]
         , event_data.value('((/event[1]/action[@name="client_hostname"]/value/text())[1])', 'NVARCHAR(255)')           AS [Client]
         , event_data.value('((/event[1]/action[@name="client_app_name"]/value/text())[1])', 'NVARCHAR(255)')           AS [App]
         , event_data.value('((/event[1]/data[@name="cpu_time"]/value/text())[1])', 'BIGINT')                           AS [CPU Time]
         , event_data.value('((/event[1]/data[@name="duration"]/value/text())[1])', 'BIGINT')                           AS [Duration]
         , event_data.value('((/event[1]/data[@name="logical_reads"]/value/text())[1])', 'BIGINT')                      AS [Logical Reads]
         , event_data.value('((/event[1]/data[@name="physical_reads"]/value/text())[1])', 'BIGINT')                     AS [Physical Reads]
         , event_data.value('((/event[1]/data[@name="writes"]/value/text())[1])', 'BIGINT')                             AS [Writes]
         , event_data.value('xs:hexBinary(((/event[1]/action[@name="query_hash"]/value/text())[1]))', 'BINARY(8)')      AS [Query Hash]
         , event_data.value('xs:hexBinary(((/event[1]/action[@name="query_plan_hash"]/value/text())[1]))', 'BINARY(8)') AS [Plan Hash]
         , event_data.value('xs:hexBinary(((/event[1]/action[@name="plan_handle"]/value/text())[1]))', 'VARBINARY(64)') AS [PlanHandle]
         , file_name
         , event_data.value('((/event[1]/data[@name="offset"]/value/text())[1])', 'BIGINT')                             AS [Stmt Offset]
         , event_data.value('((/event[1]/data[@name="offset_end"]/value/text())[1])', 'BIGINT')                         AS [Stmt Offset End]
    FROM
    (SELECT * FROM #EventData) Eventata
   )
SELECT ei.*
     , TRY_CONVERT(XML, qp.Query_Plan) AS [Plan]
INTO #Queries
FROM EventInfo ei
    OUTER APPLY sys.dm_exec_text_query_plan(ei.PlanHandle, ei.[Stmt Offset], ei.[Stmt Offset End]) qp
OPTION (MAXDOP 8, RECOMPILE);


SELECT t.event_data,
       t.Event
     , t.[Event Time]
     --, t.SessionId
     --, t.ObjectId
     , t.ObjectName
     --, t.DBId
     , t.DBName
     , t.Statement
     , t.SQL
     , t.[User Name]
     --, t.Client
     --, t.App
     , t.[CPU Time] / 1000 as cpuTime
     , t.Duration / 1000 as duration
     , t.[Logical Reads]
     , t.[Physical Reads]
     , t.Writes
     , t.[Query Hash]
     --, t.[Plan Hash]
     --, t.PlanHandle
     --, t.File_Name
     --, t.[Plan]
     , sql_text = t.event_data.value(N'(/event/action[@name="sql_text"]/value)[1]', N'nvarchar(max)')
     --, actual_plan = z.xml_fragment.query('.')
     --, 1
     --, 'PerformanceTuning_Mofid_Queries'
FROM #Queries AS t
    outer APPLY t.event_data.nodes(N'/event/data[@name="showplan_xml"]/value/*') AS z(xml_fragment)
	order by duration desc

GO