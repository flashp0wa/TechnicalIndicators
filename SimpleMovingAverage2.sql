CREATE OR ALTER PROCEDURE SimpleMovingAverage 
	@symbolId int = NULL, 
	@timeFrameId tinyint = NULL,
	@sma10Out decimal(19,8) = NULL output,
	@sma20Out decimal(19,8) = NULL output,
	@sma30Out decimal(19,8) = NULL output,
	@sma50Out decimal(19,8) = NULL output,
	@sma100Out decimal(19,8) = NULL output,
	@sma200Out decimal(19,8) = NULL output
AS
BEGIN
	BEGIN TRY
	SET NOCOUNT ON;
	SET XACT_ABORT ON;
	
	DECLARE 
		@periodIndex AS TINYINT = 1,
		@currentPeriod AS SMALLINT,
		@sqlCommand as NVARCHAR(2000),
		@preRows as SMALLINT,
		@periodCount as SMALLINT
	-- Setting up period table for required period calculation
		
	SET @periodCount = (SELECT COUNT(periodNumber) FROM itvf_TAPeriods('Simple Moving Average'))
	
	IF (SELECT COUNT(*) FROM cry_simple_moving_average) = 0 AND @symbolId IS NULL AND @timeFrameId IS NULL
	BEGIN
	WHILE @periodIndex <= @periodCount
		BEGIN
			SET @currentPeriod = (SELECT periodNumber FROM itvf_TAPeriods('Simple Moving Average') WHERE rowNumber = @periodIndex)
			SET @preRows = @currentPeriod - 1
			SET @sqlCommand = 
			CONCAT(
			'INSERT INTO cry_simple_moving_average 
				SELECT
					eventTime,
					symbolId,
					timeFrameId,
					sma,
					dataPeriod
				FROM(
					SELECT 
						AVG(closePrice) OVER(PARTITION BY symbolId, timeFrameId ORDER BY closeTime ROWS BETWEEN ', @preRows,' PRECEDING AND current ROW) sma, 
						symbolId, 
						timeFrameId, 
						dataPeriod = ', @currentPeriod,', 
						MAX(closeTime) eventTime,
						closePrice
					FROM cry_klines
					GROUP BY 
						symbolId, 
						timeFrameId, 
						closeTime, 
						closePrice	
				  ) as Base
				'
			);
			EXEC dbo.sp_executesql @sqlCommand
			SET @periodIndex +=1
		END
	END
	ELSE
	BEGIN
		DECLARE 
			@symbolCount AS INT,
			@actualSymbol AS INT,
			@symbolIndex AS INT = 1,
			@actualTimeFrame AS TINYINT,
			@lastEntryDate AS DATETIME2(0)
		
		SET @symbolCount = (SELECT COUNT(symbolId) FROM itvf_Symbols(@symbolId, @timeFrameId))
		
		WHILE @symbolIndex <= @symbolCount
		BEGIN
			SELECT @actualSymbol = (SELECT symbolId FROM itvf_Symbols(@symbolId, @timeFrameId) WHERE rowNumber = @symbolIndex),
				   @actualTimeFrame = (SELECT timeFrameId FROM itvf_Symbols(@symbolId, @timeFrameId) WHERE rowNumber = @symbolIndex)
	
			WHILE @periodIndex <= @periodCount
			BEGIN
				SET @currentPeriod = (SELECT periodNumber FROM itvf_TAPeriods('Simple Moving Average') WHERE rowNumber = @periodIndex)
				SET @preRows = @currentPeriod - 1
				SET @lastEntryDate = (SELECT max(eventTime) 
									  FROM cry_simple_moving_average
									  WHERE 
										symbolId = @actualSymbol AND 
										timeFrameId = @actualTimeFrame AND 
										dataPeriod = @currentPeriod
									  )
									  
				CREATE TABLE #temp_sma (
					closeTime DATETIME2(0),
					closePrice decimal(19,8) NULL
				)
	
				INSERT INTO #temp_sma
				SELECT closeTime, closePrice
				FROM cry_klines
				WHERE 
					symbolId = @actualSymbol AND 
					timeFrameId = @actualTimeFrame AND 
					closeTime >= ISNULL(dbo.scr_PeriodToTime(@actualTimeFrame, @currentPeriod, @lastEntryDate, 0),'1000-01-01')
		
				SET @sqlCommand = CONCAT(
				'
				INSERT INTO cry_simple_moving_average 
				SELECT 
					closeTime, 
					symbolId = ','''',@actualSymbol,'''',', 
					timeFrameId = ','''', @actualTimeFrame,'''',', 
					sma, 
					dataPeriod = ', @currentPeriod,'
				FROM (
					SELECT 
						AVG(closePrice) OVER(ORDER BY closeTime ROWS BETWEEN ', @preRows,' PRECEDING AND current ROW) sma, 
						closeTime,
						closePrice
					FROM #temp_sma
				) AS smaTable
				WHERE closeTime > ISNULL(','''',@lastEntryDate,'''',',''1000-01-01'')
				'
				)
				EXEC dbo.sp_executesql @sqlCommand

				if @symbolId is not null
				begin
					if @currentPeriod = 10
					begin
						set @sma10Out = (
							select sma
							from cry_simple_moving_average
							where 
								dataPeriod = @currentPeriod
								and eventTime = (
									select MAX(eventTime) 
									from cry_simple_moving_average
									where 
										dataPeriod = @currentPeriod
										and symbolId = @actualSymbol
										and timeFrameId = @actualTimeFrame
									)
						)
					end
					if @currentPeriod = 20
					begin
						set @sma20Out = (
							select sma
							from cry_simple_moving_average
							where 
								dataPeriod = @currentPeriod
								and eventTime = (
									select MAX(eventTime) 
									from cry_simple_moving_average
									where 
										dataPeriod = @currentPeriod
										and symbolId = @actualSymbol
										and timeFrameId = @actualTimeFrame
									)
						 )
					end
					if @currentPeriod = 30
					begin
						set @sma30Out = (
							select sma
							from cry_simple_moving_average
							where 
								dataPeriod = @currentPeriod
								and eventTime = (
									select MAX(eventTime) 
									from cry_simple_moving_average
									where dataPeriod = @currentPeriod
									)
					)
					end
					if @currentPeriod = 50
					begin
						set @sma50Out = (
							select sma
							from cry_simple_moving_average
							where 
								dataPeriod = @currentPeriod
								and eventTime = (
									select MAX(eventTime)
									from cry_simple_moving_average
									where dataPeriod = @currentPeriod
									)
						)
					end
					if @currentPeriod = 100
					begin
						set @sma100Out = (
							select sma
							from cry_simple_moving_average
							where 
								dataPeriod = @currentPeriod
								and eventTime = (
									select MAX(eventTime) 
									from cry_simple_moving_average
									where dataPeriod = @currentPeriod
									)
						)
					end
					if @currentPeriod = 200
					begin
						set @sma200Out = (
							select sma
							from cry_simple_moving_average
							where 
								dataPeriod = @currentPeriod
								and eventTime = (
									select MAX(eventTime) 
									from cry_simple_moving_average 
									where dataPeriod = @currentPeriod
									)
						)
					end
				end

				SET @periodIndex += 1
				DROP TABLE #temp_sma
			END
			SET @periodIndex = 1
			SET @symbolIndex += 1
		END
	END
	
	IF @symbolId is null
	BEGIN
		EXEC BollingerBands
	END

	END TRY
	BEGIN CATCH
		EXEC LogErrorMessage;
		THROW;
	END CATCH
END;