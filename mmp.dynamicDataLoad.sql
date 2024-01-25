DROP PROC IF EXISTS mmp.dynamicDataLoad
GO

CREATE PROC mmp.dynamicDataLoad
(
     @etlMappingName VARCHAR(50) 
    ,@sourceApplicationID INT 
    ,@targetApplicationID INT 
    ,@sourceTable VARCHAR(50) 
    ,@targetTable VARCHAR(50) 
    ,@etlCommand VARCHAR(MAX) = NULL
)

AS

SELECT DISTINCT @etlCommand = CONCAT
    (
        'INSERT INTO '
        ,targetSchema
        ,'.'
        ,targetTable
        ,'('
        , 
            (
                SELECT STUFF(
                (
                    SELECT ', ' + cast(targetColumn as varchar(max))
                    FROM mmp.etlMapping tm
                    WHERE tm.etlMappingName = @etlMappingName
                        AND tm.sourceApplicationID = @sourceApplicationID
                        AND tm.targetApplicationID = @targetApplicationID
                        AND tm.sourceTable = @sourceTable
                        AND tm.targetTable = @targetTable
                    ORDER BY sourceColumnOrder
                FOR XML PATH('')
                ), 1, 2, ''))
        ,')'
        ,' SELECT '
        , 
            (
                SELECT STUFF(
                (
                    SELECT ', ' + cast(sourceColumn as varchar(max))
                    FROM mmp.etlMapping sm
                    WHERE sm.etlMappingName = @etlMappingName
                        AND sm.sourceApplicationID = @sourceApplicationID
                        AND sm.targetApplicationID = @targetApplicationID
                        AND sm.sourceTable = @sourceTable
                        AND sm.targetTable = @targetTable
                    ORDER BY sourceColumnOrder
                FOR XML PATH('')
                ), 1, 2, '')
            )
        ,' FROM '
        ,sourceSchema
        ,'.'
        ,sourceTable
    )
FROM mmp.etlMapping m
WHERE m.etlMappingName = @etlMappingName
    AND m.sourceApplicationID = @sourceApplicationID
    AND m.targetApplicationID = @targetApplicationID
    AND m.sourceTable = @sourceTable
    AND m.targetTable = @targetTable

MERGE mmp.etlCommand AS tgt
USING 
    (VALUES
        (
             'SQL'
            ,@etlCommand
            ,@etlMappingName
            ,@sourceApplicationID
            ,@targetApplicationID
            ,@sourceTable
            ,@targetTable
        )) AS src (etlCommandType, etlCommand, etlMappingName, sourceApplicationID, targetApplicationID, sourceTable, targetTable)
ON src.etlCommandType = tgt.etlCommandType
    AND src.etlMappingName = tgt.etlMappingName
    AND src.sourceApplicationID = tgt.sourceApplicationID
    AND src.targetApplicationID = tgt.targetApplicationID
    AND src.sourceTable = tgt.sourceTable
    AND src.targetTable = tgt.targetTable
WHEN MATCHED THEN
    UPDATE SET tgt.etlCommandText = src.etlCommand
WHEN NOT MATCHED THEN
    INSERT 
    (
         etlCommandType
        ,etlCommandText
        ,etlMappingName 
        ,sourceApplicationID
        ,targetApplicationID
        ,sourceTable  
        ,targetTable  
    )
    VALUES
    (
         src.etlCommandType
        ,src.etlCommand
        ,src.etlMappingName
        ,src.sourceApplicationID
        ,src.targetApplicationID
        ,src.sourceTable
        ,src.targetTable
    );

GO

-- EXEC mmp.dynamicDataLoad
--      @etlMappingName = 'Patient'
--     ,@sourceApplicationID = 2
--     ,@targetApplicationID = 2
--     ,@sourceTable = 'person'
--     ,@targetTable = 'dimPatient'