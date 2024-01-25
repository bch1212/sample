/*
Add notes
Add logging
Add print statements
EXEC payroll.timeMachineSubtotalPaystatement @payPeriodId = 25, @asOfDate = '2023-01-26 04:00:00.000' --UTC DATE for @asOfDate

-- UTC conversion from PST
DECLARE @asOfDate DATETIME = ( SELECT CAST( CAST( '2023-02-08 13:56:09' AS DATETIME ) AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE 'UTC' AS DATETIME ) )
EXEC payroll.timeMachineSubtotalPaystatement @payPeriodId = 773, @asOfDate = @asOfDate

DROP PROCEDURE payroll.timeMachineSubtotalPaystatement
*/
CREATE PROC payroll.timeMachineSubtotalPaystatement

    @payPeriodId INT
    ,@asOfDate DATETIME 
    ,@employeeId INT    = NULL  -- employee is optional

AS

BEGIN TRY
    BEGIN TRAN
         --DECLARE
         --    @payPeriodId INT = 773
         --    ,@employeeId INT = NULL
         --    ,@asOfDate DATETIME = ( SELECT CAST( CAST( '2023-02-08 13:56:09' AS DATETIME ) AT TIME ZONE 'Pacific Standard Time' AT TIME ZONE 'UTC' AS DATETIME ) )

        DROP TABLE IF EXISTS #subtotalId;
        DROP TABLE IF EXISTS #subtotalBaseId;

        --Delete the existing values from all subtotal tables
        SELECT s.subtotalId AS id
        INTO #subtotalId
        FROM payroll.subtotal s
        JOIN payroll.vPayPeriodWithStatus p ON s.payPeriodId = p.payPeriodId 
            AND p.[status] = 'OPEN'
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)


        DELETE s 
        FROM payroll.subtotal s
        JOIN #subtotalId i ON s.subtotalId = i.id
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)

        --SELECT all subtotalBaseId's to delete
        SELECT b.subtotalBaseId AS id
        INTO #subtotalBaseId
        FROM payroll.subtotalBase b
        JOIN #subtotalId i ON b.subtotalId = i.id

        DELETE b
        FROM payroll.subtotalBase b
        JOIN #subtotalId i ON b.subtotalId = i.id

        DELETE bt
        FROM payroll.subtotalBaseCommission bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseSpiff bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseMultistepA bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseMultistepB bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseXray bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseAdjustment bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseOrthoStart bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseGuarantee bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseInvisalignStart bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseInvisalignCompletion bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseInvisalignRoll bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DELETE bt
        FROM payroll.subtotalBaseFullMonth bt
        JOIN #subtotalBaseId i ON bt.subtotalBaseId = i.id

        DECLARE @payStatementId idTableType
            ,@payStatementOfficeId idTableType
            ,@payStatementOfficeDetailId idTableType

        DELETE ps 
        OUTPUT deleted.payStatementId INTO @payStatementId
        FROM payroll.payStatement ps
        WHERE 1=1
            AND ps.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR ps.employeeId = @employeeId)

        DELETE pso
        OUTPUT deleted.payStatementOfficeId INTO @payStatementOfficeId
        FROM payroll.payStatementOffice pso
        JOIN @payStatementId ps ON pso.payStatementId = ps.id

        DELETE psod
        OUTPUT deleted.payStatementOfficeDetailId INTO @payStatementOfficeDetailId
        FROM payroll.payStatementOfficeDetail psod
        JOIN @payStatementOfficeId pso ON psod.payStatementOfficeId = pso.id
        
        --Subtotal
        SET IDENTITY_INSERT payroll.subtotal ON
        INSERT INTO payroll.subtotal
        (
        [subtotalId],[employeeId],[officeId],[payPeriodId],[contractId]
        )
        SELECT [subtotalId],[employeeId],[officeId],[payPeriodId],[contractId]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotal OFF

        --Subtotal Base
        SET IDENTITY_INSERT payroll.subtotalBase ON
        INSERT INTO payroll.subtotalBase
        (
        [subtotalBaseId],[subtotalId],[grossAmount],[subtotalAmount],[variableCompensation],[previousPayPeriodCompensation],[doctorDay],[payrollDay]
        )
        SELECT [subtotalBaseId],b.[subtotalId],[grossAmount],[subtotalAmount],[variableCompensation],[previousPayPeriodCompensation],[doctorDay],[payrollDay]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBase OFF

        --subtotalBaseCommission
        SET IDENTITY_INSERT payroll.subtotalBaseCommission ON
        INSERT INTO payroll.subtotalBaseCommission
        (
        [subtotalBaseCommissionId],[subtotalBaseId],[commissionAmount],[commissionPercent],[subtotalAmount]
        )
        SELECT [subtotalBaseCommissionId],bs.[subtotalBaseId],[commissionAmount],[commissionPercent],bs.[subtotalAmount]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseCommission FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseCommission OFF

        --subtotalBaseSpiff
        SET IDENTITY_INSERT payroll.subtotalBaseSpiff ON
        INSERT INTO payroll.subtotalBaseSpiff
        (
        [subtotalBaseSpiffId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier]
        )
        SELECT [subtotalBaseSpiffId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseSpiff FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseSpiff OFF

        --subtotalBaseMultistepA
        SET IDENTITY_INSERT payroll.subtotalBaseMultistepA ON
        INSERT INTO payroll.subtotalBaseMultistepA
        (
        [subtotalBaseMultistepAId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier]
        )
        SELECT [subtotalBaseMultistepAId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseMultistepA FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseMultistepA OFF

        --subtotalBaseMultistepB
        SET IDENTITY_INSERT payroll.subtotalBaseMultistepB ON
        INSERT INTO payroll.subtotalBaseMultistepB
        (
        [subtotalBaseMultistepBId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier]
        )
        SELECT [subtotalBaseMultistepBId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseMultistepB FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseMultistepB OFF

        --subtotalBaseXray
        SET IDENTITY_INSERT payroll.subtotalBaseXray ON
        INSERT INTO payroll.subtotalBaseXray
        (
        [subtotalBaseXrayId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier]
        )
        SELECT [subtotalBaseXrayId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseXray FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseXray OFF

        --subtotalBaseAdjustment
        SET IDENTITY_INSERT payroll.subtotalBaseAdjustment ON
        INSERT INTO payroll.subtotalBaseAdjustment
        (
        [subtotalBaseAdjustmentId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier],[note],[adjustmentId],[earningReasonCodeId]
        )
        SELECT [subtotalBaseAdjustmentId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier],[note],[adjustmentId],[earningReasonCodeId]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseAdjustment FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseAdjustment OFF

        --subtotalBaseOrthoStart
        SET IDENTITY_INSERT payroll.subtotalBaseOrthoStart ON
        INSERT INTO payroll.subtotalBaseOrthoStart
        (
        [subtotalBaseOrthoStartId],[subtotalBaseId],[cntStarts],[bonusThreshold],[cntOrthoStartBonusLevel1],[orthoStartBonusLevel1],[cntOrthoStartBonusLevel2],[orthoStartBonusLevel2]
        )
        SELECT [subtotalBaseOrthoStartId],bs.[subtotalBaseId],[cntStarts],[bonusThreshold],[cntOrthoStartBonusLevel1],[orthoStartBonusLevel1],[cntOrthoStartBonusLevel2],[orthoStartBonusLevel2]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseOrthoStart FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseOrthoStart OFF

        --subtotalBaseGuarantee
        SET IDENTITY_INSERT payroll.subtotalBaseGuarantee ON
        INSERT INTO payroll.subtotalBaseGuarantee
        (
        [subtotalBaseGuaranteeId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier]
        )
        SELECT [subtotalBaseGuaranteeId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseGuarantee FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseGuarantee OFF

        --subtotalBaseFullMonth
        SET IDENTITY_INSERT payroll.subtotalBaseFullMonth ON
        INSERT INTO payroll.subtotalBaseFullMonth
        (
        [subtotalBaseFullMonthId],[subtotalBaseId],[subtotalAmount]
        )
        SELECT bs.[subtotalBaseFullMonthId],bs.[subtotalBaseId],bs.[subtotalAmount]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseFullMonth FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseFullMonth OFF

        --subtotalBaseInvisalignStart
        SET IDENTITY_INSERT payroll.subtotalBaseInvisalignStart ON
        INSERT INTO payroll.subtotalBaseInvisalignStart
        (
        [subtotalBaseInvisalignStartId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier]
        )
        SELECT [subtotalBaseInvisalignStartId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseInvisalignStart FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseInvisalignStart OFF

        --subtotalBaseInvisalignCompletion
        SET IDENTITY_INSERT payroll.subtotalBaseInvisalignCompletion ON
        INSERT INTO payroll.subtotalBaseInvisalignCompletion
        (
        [subtotalBaseInvisalignCompletionId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier]
        )
        SELECT [subtotalBaseInvisalignCompletionId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseInvisalignCompletion FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseInvisalignCompletion OFF

        --subtotalBaseInvisalignRoll
        SET IDENTITY_INSERT payroll.subtotalBaseInvisalignRoll ON
        INSERT INTO payroll.subtotalBaseInvisalignRoll
        (
        [subtotalBaseInvisalignRollId],[subtotalBaseId],[prodAmount],[spiffAmount],[subtotalAmount],[cntModifier]
        )
        SELECT [subtotalBaseInvisalignRollId],bs.[subtotalBaseId],[prodAmount],[spiffAmount],bs.[subtotalAmount],[cntModifier]
        FROM payroll.subtotal FOR SYSTEM_TIME AS OF @asOfDate s
        JOIN payroll.subtotalBase FOR SYSTEM_TIME AS OF @asOfDate b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseInvisalignRoll FOR SYSTEM_TIME AS OF @asOfDate bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND s.payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.subtotalBaseInvisalignRoll OFF

        --Pay statement
        SET IDENTITY_INSERT payroll.payStatement ON
        INSERT INTO payroll.payStatement
        (
            [payStatementId],[employeeId],[payPeriodId],[contractId]
        )
        SELECT [payStatementId],[employeeId],[payPeriodId],[contractId]
        FROM payroll.payStatement FOR SYSTEM_TIME AS OF @asOfDate p
        WHERE 1=1
            AND payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR p.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.payStatement OFF

        --Pay statement office
        SET IDENTITY_INSERT payroll.payStatementOffice ON
        INSERT INTO payroll.payStatementOffice
        (
            [payStatementOfficeId],[payStatementId],[officeId]
        )
        SELECT o.[payStatementOfficeId],o.[payStatementId],o.[officeId]
        FROM payroll.payStatement FOR SYSTEM_TIME AS OF @asOfDate p
        JOIN payroll.payStatementOffice FOR SYSTEM_TIME AS OF @asOfDate o ON p.payStatementId = o.payStatementId
        WHERE 1=1
            AND payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR p.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.payStatementOffice OFF

        --Pay statement office detail
        SET IDENTITY_INSERT payroll.payStatementOfficeDetail ON
        INSERT INTO payroll.payStatementOfficeDetail
        (
            [payStatementOfficeDetailId],[payStatementOfficeId],[payStatementDetailTypeId],[earningReasonCodeId],[officeId],[calculationText],[subtotalAmount],[itemNote]
        )
        SELECT od.[payStatementOfficeDetailId],od.[payStatementOfficeId],od.[payStatementDetailTypeId],od.[earningReasonCodeId],od.[officeId],od.[calculationText],od.[subtotalAmount],od.[itemNote]
        FROM payroll.payStatement FOR SYSTEM_TIME AS OF @asOfDate p
        JOIN payroll.payStatementOffice FOR SYSTEM_TIME AS OF @asOfDate o ON p.payStatementId = o.payStatementId
        JOIN payroll.payStatementOfficeDetail FOR SYSTEM_TIME AS OF @asOfDate od ON o.payStatementOfficeId = od.payStatementOfficeId
        WHERE 1=1
            AND payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR p.employeeId = @employeeId)
        SET IDENTITY_INSERT payroll.payStatementOfficeDetail OFF


        /* --   TODO: update the pay period lock date and user
        UPDATE p 
        SET     lockedDate      = CAST( ( @asOfDate AT TIME ZONE 'UTC' AT TIME ZONE 'Pacific Standard Time' ) AS DATETIME )     -- convert UTC back to Pacific Standard Time (should use server zone?)
            ,   lockedByUserId  = 0     -- system user
        FROM payroll.payperiod AS p
        WHERE 1=1
            AND payPeriodId = @payPeriodId
        */

        SELECT * 
        FROM payroll.subtotal s
        JOIN payroll.subtotalBase b ON s.subtotalId = b.subtotalId
        JOIN payroll.subtotalBaseCommission bs ON b.subtotalBaseId = bs.subtotalBaseId
        WHERE 1=1
            AND payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR s.employeeId = @employeeId)

        SELECT * 
        FROM payroll.vPayStatement p 
        WHERE 1=1
            AND payPeriodId = @payPeriodId
            AND (@employeeId IS NULL OR p.employeeId = @employeeId)
    COMMIT TRAN
END TRY
BEGIN CATCH
    SELECT
        ERROR_NUMBER() AS ErrorNumber,
        ERROR_STATE() AS ErrorState,
        ERROR_SEVERITY() AS ErrorSeverity,
        ERROR_PROCEDURE() AS ErrorProcedure,
        ERROR_LINE() AS ErrorLine,
        ERROR_MESSAGE() AS ErrorMessage

    ROLLBACK TRAN
END CATCH
