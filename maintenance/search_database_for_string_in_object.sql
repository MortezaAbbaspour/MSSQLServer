-- Multi-database object definition search
DECLARE 
    @SearchString        NVARCHAR(4000) = N'YourSearchString',            -- raw term(s) separated by space or comma
    @DatabaseLikePattern NVARCHAR(128)  = N'%',                           -- pattern for DB names
    @ObjectTypes         NVARCHAR(100)  = N'P,FN,IF,TF,TR,V',             -- comma list of type codes
    @RequireAllTokens    BIT            = 1,                              -- 1: all tokens must match; 0: any token
    @IncludeEncrypted    BIT            = 1,                              -- show encrypted modules (no definition)
    @PreviewPadding      INT            = 60,                             -- chars before match
    @PreviewLength       INT            = 180,                            -- total snippet length
    @AutoCleanup         BIT            = 0;                              -- drop #Results at end if 1

IF OBJECT_ID('tempdb..#Results') IS NULL
BEGIN
    CREATE TABLE #Results(
        DatabaseName   NVARCHAR(128),
        /* ==================================================================================================
           Multi-Database Object Definition Search Utility
           --------------------------------------------------------------------------------------------------
           Features:
             * Searches across all online user databases (filterable by LIKE pattern)
             * Supports multi-token search (space/comma separated) with ALL or ANY matching semantics
             * Safe parameterization (avoids direct concatenation of search text into dynamic SQL)
             * Provides match-centered snippet, object metadata (create/modify dates), encryption flag
             * Optional inclusion of encrypted modules (definition not visible)
             * Object type filtering via comma list (default: P,FN,IF,TF,TR,V)
             * Retains results in #Results unless @AutoCleanup = 1
           --------------------------------------------------------------------------------------------------
           Adjust the initial parameter defaults below as needed.
        ================================================================================================== */

        SET NOCOUNT ON;

        DECLARE 
            @SearchString        NVARCHAR(4000) = N'YourSearchString',  -- Enter tokens (space or comma separated)
            @DatabaseLikePattern NVARCHAR(128)  = N'%',                 -- Pattern for database names (e.g. 'App_%')
            @ObjectTypes         NVARCHAR(100)  = N'P,FN,IF,TF,TR,V',   -- Comma list of object type codes
            @RequireAllTokens    BIT            = 1,                    -- 1 = all tokens must match; 0 = any token
            @IncludeEncrypted    BIT            = 1,                    -- 1 = include encrypted modules (definition NULL)
            @PreviewPadding      INT            = 60,                   -- Chars shown before first match
            @PreviewLength       INT            = 180,                  -- Total snippet length
            @AutoCleanup         BIT            = 0;                    -- 1 = drop #Results at end

        IF @SearchString IS NULL OR LTRIM(RTRIM(@SearchString)) = ''
        BEGIN
            RAISERROR('Search string cannot be empty.',16,1);
            RETURN;
        END

        -- Token parsing: split by space or comma
        IF OBJECT_ID('tempdb..#Tokens') IS NOT NULL DROP TABLE #Tokens;
        CREATE TABLE #Tokens(Token NVARCHAR(4000) NOT NULL PRIMARY KEY);

        DECLARE @Work NVARCHAR(4000) = REPLACE(REPLACE(@SearchString, ',', ' '), CHAR(9), ' ');
        WHILE LEN(@Work) > 0
        BEGIN
            DECLARE @One NVARCHAR(4000) = LEFT(@Work + ' ', CHARINDEX(' ', @Work + ' ') - 1);
            IF @One <> '' INSERT INTO #Tokens(Token) VALUES(@One);
            SET @Work = LTRIM(SUBSTRING(@Work + ' ', LEN(@One) + 1, 4000));
        END

        IF NOT EXISTS(SELECT 1 FROM #Tokens)
        BEGIN
            RAISERROR('No valid tokens parsed from @SearchString.',16,1);
            RETURN;
        END

        -- Object type list
        IF OBJECT_ID('tempdb..#ObjectTypes') IS NOT NULL DROP TABLE #ObjectTypes;
        CREATE TABLE #ObjectTypes(Code CHAR(2) PRIMARY KEY);
        INSERT INTO #ObjectTypes(Code)
        SELECT DISTINCT UPPER(LTRIM(RTRIM(value)))
        FROM STRING_SPLIT(@ObjectTypes, ',')
        WHERE LTRIM(RTRIM(value)) <> '';

        IF NOT EXISTS(SELECT 1 FROM #ObjectTypes)
        BEGIN
            RAISERROR('No valid object types specified in @ObjectTypes.',16,1);
            RETURN;
        END

        -- Results table
        IF OBJECT_ID('tempdb..#Results') IS NULL
        BEGIN
            CREATE TABLE #Results(
                DatabaseName    NVARCHAR(128),
                SchemaName      NVARCHAR(128),
                ObjectName      NVARCHAR(128),
                ObjectTypeCode  CHAR(2),
                ObjectTypeDesc  NVARCHAR(60),
                IsEncrypted     BIT,
                MatchPosition   INT NULL,
                MatchSnippet    NVARCHAR(4000) NULL,
                CreatedDate     DATETIME,
                ModifiedDate    DATETIME
            );
        END

        -- Candidate databases
        IF OBJECT_ID('tempdb..#DBs') IS NOT NULL DROP TABLE #DBs;
        CREATE TABLE #DBs(DBName NVARCHAR(128) PRIMARY KEY);
        INSERT INTO #DBs(DBName)
        SELECT name
        FROM sys.databases
        WHERE database_id > 4            -- exclude system
          AND state = 0                  -- online
          AND name LIKE @DatabaseLikePattern
          AND name NOT IN ('master','model','msdb','tempdb');

        DECLARE @DBName NVARCHAR(128);
        DECLARE db_cur CURSOR FAST_FORWARD FOR SELECT DBName FROM #DBs;
        OPEN db_cur; FETCH NEXT FROM db_cur INTO @DBName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @SQL NVARCHAR(MAX) = N'
                USE ' + QUOTENAME(@DBName) + N';
                SELECT 
                    DB_NAME()              AS DatabaseName,
                    s.name                 AS SchemaName,
                    o.name                 AS ObjectName,
                    o.type                 AS ObjectTypeCode,
                    o.type_desc            AS ObjectTypeDesc,
                    CASE WHEN m.definition IS NULL THEN 1 ELSE 0 END AS IsEncrypted,
                    CASE WHEN m.definition IS NULL THEN NULL ELSE PATINDEX(''%'' + (SELECT TOP(1) Token FROM #Tokens ORDER BY Token) + ''%'', m.definition) END AS MatchPosition,
                    o.create_date          AS CreatedDate,
                    o.modify_date          AS ModifiedDate,
                    m.definition           AS FullDef
                FROM sys.objects o
                LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
                INNER JOIN sys.schemas s     ON o.schema_id = s.schema_id
                WHERE o.is_ms_shipped = 0
                  AND o.type IN (SELECT Code FROM #ObjectTypes)
                  ' + CASE WHEN @IncludeEncrypted = 1 THEN N'' ELSE N' AND m.definition IS NOT NULL' END + N'
                  AND ' + CASE WHEN @RequireAllTokens = 1 
                                THEN N'NOT EXISTS (SELECT 1 FROM #Tokens t WHERE m.definition NOT LIKE ''%'' + t.Token + ''%'')'
                                ELSE N'EXISTS (SELECT 1 FROM #Tokens t WHERE m.definition LIKE ''%'' + t.Token + ''%'')' END + N';';

            BEGIN TRY
                DECLARE @Stage TABLE(
                    DatabaseName   NVARCHAR(128),
                    SchemaName     NVARCHAR(128),
                    ObjectName     NVARCHAR(128),
                    ObjectTypeCode CHAR(2),
                    ObjectTypeDesc NVARCHAR(60),
                    IsEncrypted    BIT,
                    MatchPosition  INT NULL,
                    CreatedDate    DATETIME,
                    ModifiedDate   DATETIME,
                    FullDef        NVARCHAR(MAX)
                );

                INSERT INTO @Stage
                EXEC sys.sp_executesql @SQL;  -- definitions accessed in context of each DB

                INSERT INTO #Results(
                    DatabaseName, SchemaName, ObjectName, ObjectTypeCode, ObjectTypeDesc,
                    IsEncrypted, MatchPosition, MatchSnippet, CreatedDate, ModifiedDate)
                SELECT 
                    DatabaseName, SchemaName, ObjectName, ObjectTypeCode, ObjectTypeDesc,
                    IsEncrypted, MatchPosition,
                    CASE 
                        WHEN MatchPosition IS NULL OR FullDef IS NULL THEN NULL
                        ELSE SUBSTRING(
                                FullDef,
                                CASE WHEN MatchPosition <= @PreviewPadding THEN 1 ELSE MatchPosition - @PreviewPadding END,
                                @PreviewLength
                             )
                    END AS MatchSnippet,
                    CreatedDate, ModifiedDate
                FROM @Stage;
            END TRY
            BEGIN CATCH
                PRINT CONCAT('Error searching ', @DBName, ': ', ERROR_NUMBER(), ' - ', ERROR_MESSAGE());
            END CATCH

            FETCH NEXT FROM db_cur INTO @DBName;
        END

        CLOSE db_cur; DEALLOCATE db_cur;

        -- Summary
        SELECT DatabaseName, COUNT(*) AS MatchCount
        FROM #Results
        GROUP BY DatabaseName
        ORDER BY MatchCount DESC, DatabaseName;

        -- Detailed results
        SELECT *
        FROM #Results
        ORDER BY DatabaseName, SchemaName, ObjectTypeDesc, ObjectName;

        -- Example: inspect a specific object further
        -- SELECT * FROM #Results WHERE ObjectName = 'ProcedureName';

        IF @AutoCleanup = 1 DROP TABLE #Results;

        SET NOCOUNT OFF;
    INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
    WHERE o.is_ms_shipped = 0
      ' + @ObjectTypePredicate + N'
      ' + CASE WHEN @IncludeEncrypted = 1 THEN N'' ELSE N' AND m.definition IS NOT NULL' END + N'
      AND ' + @Predicate + N';';

    -- Build parameter list: one parameter per token + primary token for PATINDEX
    DECLARE @ParamDef NVARCHAR(MAX) = N'@PrimaryToken NVARCHAR(4000)';
    DECLARE @i INT = 1, @cnt INT = (SELECT COUNT(*) FROM @Tokens);
    WHILE @i <= @cnt
    BEGIN
        SET @ParamDef += N', @T' + CAST(@i AS NVARCHAR(10)) + N' NVARCHAR(4000)';
        SET @i += 1;
    END

    DECLARE @PrimaryToken NVARCHAR(4000) = (SELECT TOP(1) Token FROM @Tokens ORDER BY Token);

    -- Assign token values
    DECLARE @ParamValues NVARCHAR(MAX) = N'';
    -- We'll execute with sp_executesql passing parameters individually
    DECLARE @TokenIndex INT = 1;
    DECLARE @ExecSQL NVARCHAR(MAX) = @SQL;

    -- Prepare actual execution
    DECLARE @Token1 NVARCHAR(4000), @Token2 NVARCHAR(4000), @Token3 NVARCHAR(4000), @Token4 NVARCHAR(4000), @Token5 NVARCHAR(4000); -- extend if needed
    -- (Alternatively loop with table-valued parameter if using a wrapper proc.)

    -- Simplify: directly build dynamic exec with tokens from table variable
    DECLARE @DynParams NVARCHAR(MAX) = @ParamDef;
    DECLARE @DynExecParams NVARCHAR(MAX) = N'@PrimaryToken=''' + @PrimaryToken + N'''';

    ;WITH OrderedTokens AS (
        SELECT Token, rn = ROW_NUMBER() OVER(ORDER BY Token) FROM @Tokens
    )
    SELECT @DynExecParams = @DynExecParams + N', @T' + CAST(rn AS NVARCHAR(10)) + N'=''' + Token + N''''
    FROM OrderedTokens;

    DECLARE @InsertWrapper NVARCHAR(MAX) = N'
    INSERT INTO #StageResult
    ' + @ExecSQL;

    -- Stage table to transform snippet
    IF OBJECT_ID('tempdb..#StageResult') IS NOT NULL DROP TABLE #StageResult;
    CREATE TABLE #StageResult(
        DatabaseName NVARCHAR(128),
        SchemaName NVARCHAR(128),
        ObjectName NVARCHAR(128),
        ObjectTypeCode CHAR(2),
        ObjectTypeDesc NVARCHAR(60),
        IsEncrypted BIT,
        MatchPositionPrimary INT NULL,
        CreatedDate DATETIME,
        ModifiedDate DATETIME,
        FullDef NVARCHAR(MAX)
    );

    BEGIN TRY
        EXEC sp_executesql @InsertWrapper, @DynParams, @PrimaryToken=@PrimaryToken
            -- NOTE: Parameter passing simplified; for production expand to all tokens safely
        ;
    END TRY
    BEGIN CATCH
        PRINT CONCAT('Error searching ', @DBName, ': ', ERROR_NUMBER(), ' - ', ERROR_MESSAGE());
    END CATCH

    -- Insert transformed rows
    INSERT INTO #Results(
        DatabaseName, SchemaName, ObjectName, ObjectTypeCode, ObjectTypeDesc,
        IsEncrypted, MatchPosition, MatchSnippet, CreatedDate, ModifiedDate
    )
    SELECT
        DatabaseName, SchemaName, ObjectName, ObjectTypeCode, ObjectTypeDesc,
        IsEncrypted,
        MatchPositionPrimary,
        CASE 
            WHEN MatchPositionPrimary IS NULL OR FullDef IS NULL THEN NULL
            ELSE SUBSTRING(
                    FullDef,
                    CASE WHEN MatchPositionPrimary <= @PreviewPadding THEN 1 ELSE MatchPositionPrimary - @PreviewPadding END,
                    @PreviewLength
                 )
        END AS MatchSnippet,
        CreatedDate, ModifiedDate
    FROM #StageResult;

    DROP TABLE IF EXISTS #StageResult;

    FETCH NEXT FROM db_cur INTO @DBName;
END

CLOSE db_cur; DEALLOCATE db_cur;

-- Summary
SELECT DatabaseName, COUNT(*) AS Matches
FROM #Results
GROUP BY DatabaseName
ORDER BY Matches DESC;

-- Detailed matches
SELECT *
FROM #Results
ORDER BY DatabaseName, SchemaName, ObjectTypeDesc, ObjectName;

IF @AutoCleanup = 1
    DROP TABLE #Results;