/*
EXEC mmp.officeProcess
*/
CREATE OR ALTER PROC mmp.officeProcess

AS

--subscription variables
DECLARE @etlMappingName VARCHAR(50) = 'Office'
    ,@officeSourceApplicationID INT = 4
    ,@officeTargetApplicationID INT = 2
    ,@officeSourceTable VARCHAR(50) = 'office'
    ,@officeTargetTable VARCHAR(50) = 'office'
    ,@officeSourceSchema VARCHAR(50) = 'monday'
    ,@officeTargetSchema VARCHAR(50) = 'mmp'
    ,@etlCommandType VARCHAR(50) = 'SQL'
    ,@officeCommand VARCHAR(MAX) = NULL

--logging variables
    ,@loggingExecutionID UNIQUEIDENTIFIER
    ,@rowCount INT
    ,@startProcessing DATETIME2
    ,@finishProcessing DATETIME2

-- ETL command text to insert records from nextgen.patient_subscription_ into mmp.patientSubscription
SELECT @officeCommand = etlCommandText
FROM mmp.etlCommand
WHERE sourceApplicationID = @officeSourceApplicationID
    AND targetApplicationID = @officeTargetApplicationID
    AND etlCommandType = @etlCommandType
    AND etlMappingName = @etlMappingName

--Start data transfer
SELECT @startProcessing = getDate()
    ,@loggingExecutionID = NEWID();

TRUNCATE TABLE mmp.office;

EXEC (@officeCommand)

SELECT @rowCount = @@ROWCOUNT
    ,@finishProcessing = getDate()

--Log records   
EXEC mmp.insertLogging @loggingExecutionID
    ,@officeSourceApplicationID
    ,@officeSourceSchema
    ,@officeSourceTable
    ,@officeTargetApplicationID
    ,@officeTargetSchema
    ,@officeTargetTable
    ,@rowCount
    ,@startProcessing
    ,@finishProcessing

GO
