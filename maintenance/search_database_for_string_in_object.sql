USE MofidV2
GO

-- =============================================
-- Search all databases for a string in object definitions
-- =============================================
DECLARE @SearchString NVARCHAR(255) = 'YourSearchString';  -- <<< CHANGE THIS

-- Temp table to store results
IF OBJECT_ID('tempdb..#Results') IS NOT NULL DROP TABLE #Results;
CREATE TABLE #Results (
    DatabaseName NVARCHAR(128),
    SchemaName NVARCHAR(128),
    ObjectName NVARCHAR(128),
    ObjectType NVARCHAR(60),
    Definition NVARCHAR(MAX)
);

-- Cursor to loop through all user databases
DECLARE @DBName NVARCHAR(128);
DECLARE @SQL NVARCHAR(MAX);

DECLARE db_cursor CURSOR FOR 
SELECT name 
FROM sys.databases 
WHERE database_id > 4  -- Exclude system databases
  AND state = 0        -- Only online databases
  AND name NOT IN ('master', 'model', 'msdb', 'tempdb');  -- Optional extra filter

OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @DBName;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @SQL = '
    USE [' + @DBName + '];
    INSERT INTO #Results (DatabaseName, SchemaName, ObjectName, ObjectType, Definition)
    SELECT 
        DB_NAME() AS DatabaseName,
        s.name AS SchemaName,
        o.name AS ObjectName,
        o.type_desc AS ObjectType,
        m.definition
    FROM sys.sql_modules m
    INNER JOIN sys.objects o ON m.object_id = o.object_id
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    WHERE m.definition LIKE ''%' + @SearchString + '%''
      AND o.type IN (''P'', ''FN'', ''IF'', ''TF'', ''TR'', ''V'')  -- Procedures, Functions, Triggers, Views
    ORDER BY o.type_desc, s.name, o.name;
    ';

    BEGIN TRY
        EXEC sp_executesql @SQL;
    END TRY
    BEGIN CATCH
        PRINT 'Error searching database: ' + @DBName;
    END CATCH

    FETCH NEXT FROM db_cursor INTO @DBName;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;

-- Return results
SELECT 
    DatabaseName,
    SchemaName,
    ObjectName,
    ObjectType,
    LEFT(Definition, 100) + '...' AS Definition_Preview
FROM #Results
ORDER BY DatabaseName, SchemaName, ObjectName;

-- Optional: Show full definition for a specific object
-- SELECT * FROM #Results WHERE ObjectName = 'YourProcName';

DROP TABLE #Results;