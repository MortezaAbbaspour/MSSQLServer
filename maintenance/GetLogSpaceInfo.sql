use master
go
create or alter procedure GetLogSpaceInfo
    @DatabaseName VARCHAR(128),
    @LogSizeMB INT OUTPUT,
    @LogSpaceUsedPercent INT OUTPUT
AS
BEGIN
    DECLARE @LogSpace TABLE
    (
        DatabaseName VARCHAR(128),
        LogSizeMB FLOAT,
        LogSpaceUsedPercent FLOAT,
        Status INT
    );

    -- Insert the output of DBCC SQLPERF (LOGSPACE) into the temporary table
    INSERT INTO @LogSpace
    EXEC ('DBCC SQLPERF (LOGSPACE)');

    -- Select the LogSpaceUsedPercent for the specified database
    SELECT  @LogSizeMB = LogSizeMB,
            @LogSpaceUsedPercent = LogSpaceUsedPercent
    FROM @LogSpace
    WHERE DatabaseName = @DatabaseName;

    ---- Select all information from the temporary table
    --SELECT DatabaseName,
    --       LogSizeMB,
    --       LogSpaceUsedPercent,
    --       Status
    --FROM @LogSpace;

    -- Print the LogSpaceUsedPercent to verify (optional)
    --PRINT 'Log Space Used (%): ' + CAST(@LogSpaceUsedPercent AS VARCHAR(50));
	/*

	DECLARE @LogSpaceUsedPercent FLOAT;
	EXEC GetLogSpaceInfo @DatabaseName = 'rlcserverhelper2', @LogSpaceUsedPercent = @LogSpaceUsedPercent OUTPUT;
	PRINT 'Log Space Used (%): ' + CAST(@LogSpaceUsedPercent AS VARCHAR(50));

	*/
END;