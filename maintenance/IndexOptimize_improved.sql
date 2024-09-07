CREATE OR ALTER PROCEDURE IndexOptimize_improved
    @databaseName VARCHAR(2000),
    @LogSpaceUsedThreshold FLOAT,
    @createCommand BIT,
    @startTimeLimit TIME,
    @endTimeLimit TIME
AS
BEGIN
    DECLARE @startTime DATETIME2(7) = GETDATE();
    DECLARE @command NVARCHAR(2000) = '';
    DECLARE @commandID INT;

    -- Execute the IndexOptimize procedure, but do not run it.
    -- Instead, log the commands in the master..commandLog table with StartTime showing the time of creating the commands and EndTime having the same value.
    IF @createCommand = 1
    BEGIN
        EXECUTE [master].[dbo].[IndexOptimize] @Databases = @databaseName,
                                               @UpdateStatistics = NULL,
                                               @StatisticsSample = 100,
                                               @FragmentationMedium = 'INDEX_REORGANIZE',
                                               @FragmentationHigh = 'INDEX_REBUILD_ONLINE, INDEX_REORGANIZE',
                                               @SortInTempdb = 'Y',
                                               @MaxDOP = 12,
                                               @LogToTable = 'Y',
                                               @Execute = 'N';
    END

    -- Declare a CURSOR for the ALTER_INDEX commands that are inserted into master..commandLog after the above instance of the IndexOptimize procedure is run.
    DECLARE cursorIndexOptimize CURSOR FOR
    SELECT command,
           ID
    FROM [master].[dbo].[CommandLog]
    WHERE DatabaseName = @databaseName
          AND CommandType = 'ALTER_INDEX'
          AND ErrorNumber IS NULL
          -- Conditionally include StartTime filter
          AND (
                  @createCommand = 0
                  OR StartTime > @startTime
              )
    ORDER BY 
    CAST(ExtendedInfo.value('(/ExtendedInfo/PageCount)[1]', 'INT') AS INT) DESC,
    CAST(ExtendedInfo.value('(/ExtendedInfo/Fragmentation)[1]', 'FLOAT') AS FLOAT) DESC;

    OPEN cursorIndexOptimize;
    FETCH NEXT FROM cursorIndexOptimize
    INTO @command,
         @commandID;

    DECLARE @scriptAlterIndex NVARCHAR(2000) = '';

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @scriptAlterIndex = 'USE ' + @databaseName + '; 
        ' + @command;

        -- Check if the current time is within the specified time limits
        IF CONVERT(TIME, GETDATE())
           BETWEEN @startTimeLimit AND @endTimeLimit
        BEGIN
            -- Execute the ALTER_INDEX script in a TRY..CATCH block.
            -- If it succeeds, update the relevant row specified by ID with 0 as ErrorNumber and the corrected EndTime.
            -- If an error occurs, update the relevant row with the ErrorNumber and ErrorMessage that were raised.
            BEGIN TRY
                UPDATE master.[dbo].[CommandLog]
                SET StartTime = GETDATE()
                WHERE ID = @commandID;

                EXEC sp_executesql @scriptAlterIndex;

                UPDATE master.[dbo].[CommandLog]
                SET EndTime = GETDATE(),
                    ErrorNumber = 0,
                    ErrorMessage = 'Done.'
                WHERE ID = @commandID;
            END TRY
            BEGIN CATCH
                UPDATE master.[dbo].[CommandLog]
                SET ErrorNumber = ERROR_NUMBER(),
                    ErrorMessage = ERROR_MESSAGE()
                WHERE ID = @commandID;
            END CATCH;

            SET @scriptAlterIndex = 'USE ' + @databaseName + '; 
            CHECKPOINT;';
            EXEC sp_executesql @scriptAlterIndex;
            SET @scriptAlterIndex = '';
            DECLARE @logReuseWaitDesc VARCHAR(100) = '';
            SELECT @logReuseWaitDesc = log_reuse_wait_desc
                FROM sys.databases
                WHERE name = @databaseName;
            DECLARE @LogSpaceUsedPercent FLOAT;
            EXEC GetLogSpaceInfo @DatabaseName = @databaseName, @LogSpaceUsedPercent = @LogSpaceUsedPercent OUTPUT;
            -- After executing the ALTER_INDEX command, if log_reuse_wait_desc is anything except 'NOTHING' or if the percentage of log space filled is over 75%, a log backup should be run.
            WHILE @LogSpaceUsedPercent > @LogSpaceUsedThreshold
            BEGIN
                -- If log_reuse_wait_desc is AVAILABILITY_REPLICA, the WHILE loop waits 3 minutes and checks it repeatedly until it is resolved.
                IF @logReuseWaitDesc <> 'AVAILABILITY_REPLICA'
                BEGIN
                    EXECUTE master.[dbo].[DatabaseBackup] @Databases = @databaseName,
                                                          @Directory = 'R:\Backup',
                                                          @BackupType = 'LOG',
                                                          @CleanupTime = 720,
                                                          @Compress = 'Y',
                                                          @Verify = 'Y',
                                                          @CheckSum = 'Y',
                                                          @LogToTable = 'Y';
                END
                ELSE
                BEGIN
                    WAITFOR DELAY '00:03:00';
                    SELECT @logReuseWaitDesc = log_reuse_wait_desc
                        FROM sys.databases
                        WHERE name = @databaseName;
                END
            END
        END

        -- Fetch the next command
        FETCH NEXT FROM cursorIndexOptimize
        INTO @command,
             @commandID;
    END;

    CLOSE cursorIndexOptimize;
    DEALLOCATE cursorIndexOptimize;

/*

EXEC IndexOptimize_improved @databaseName = 'RLCServerHelper2',
                            @LogSpaceUsedThreshold = 75,    -- Example value for log space used percent
                            @createCommand = 1,           -- TRUE
                            @startTimeLimit = '22:00:00', -- 10 PM
                            @endTimeLimit = '06:00:00';   -- 6 AM

*/

END;