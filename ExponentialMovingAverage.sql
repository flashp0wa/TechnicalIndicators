create or alter procedure ExponentialMovingAverage 
	@symbolId int = NULL, 
	@timeFrameId tinyint = NULL,
	@ema50Out decimal(19,8) = NULL output,
	@ema9Out decimal(19,8) = NULL output,
	@ema200Out decimal(19,8) = NULL output,
	@ema10Out decimal(19,8) = NULL output,
	@ema12Out decimal(19,8) = NULL output,
	@ema26Out decimal(19,8) = NULL output,
	@macd1226Out decimal(19,8) = NULL output

as
begin
set xact_abort on;
set nocount on;

begin try
	declare @actualSymbol as int,
			@actualPeriod as smallint,
			@symbolIndex as int = 1,
			@actualTimeFrame as tinyint,
			@lastEntryDate as datetime2(0),
			@ema as decimal(19,8),
			@smoothingValue as tinyint = 2,
			@currentClosePrice as decimal(19,8)
	
	drop table if exists #temp_ema;
	
	while @symbolIndex <= (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
	begin
		declare @periodIndex smallint = 1,
				@prevEma decimal(19,8)
				
	
		set @actualSymbol = (select symbolId 
							 from itvf_Symbols(@symbolId, @timeFrameId) 
							 where rowNumber = @symbolIndex
							 )

		set @actualTimeFrame = (select timeFrameId 
								from itvf_Symbols(@symbolId, @timeFrameId) 
								where rowNumber = @symbolIndex
								)
		
		while @periodIndex <= (select COUNT(periodNumber) from itvf_TAPeriods('Exponential Moving Average'))
		begin
	
			declare @emaCalcIndex int = 1,
					@smoothingConstant decimal(5,4)
			
			set @actualPeriod = (
				select periodNumber 
				from itvf_TAPeriods('Exponential Moving Average') 
				where rowNumber = @periodIndex
				)
			set @smoothingConstant = cast(@smoothingValue as decimal(5,4)) / cast((@actualPeriod + 1) as decimal(8,4))
			set @lastEntryDate = (select max(eventTime)
								  from cry_exponential_moving_average 
								  where symbolId = @actualSymbol and 
									timeFrameId = @actualTimeFrame and 
									dataPeriod = @actualPeriod
								  )
			
			create table #temp_ema (
			closeTime datetime2(0),
			closePrice decimal(19,8),
			ema decimal(19,8) null,
			rowNumber int
			)
	
			insert into #temp_ema (
				closeTime, 
				closePrice, 
				rowNumber
				)
			select 
				closeTime, 
				closePrice, 
				ROW_NUMBER() over(order by closeTime)
			from cry_klines
			where timeFrameId = @actualTimeFrame and 
					symbolId = @actualSymbol and 
					closeTime > ISNULL(@lastEntryDate, '1000-01-01')
	
			if @lastEntryDate is not null
			begin
				set @prevEma = (select ema 
								from cry_exponential_moving_average
								where symbolId = @actualSymbol and 
									timeFrameId = @actualTimeFrame and 
									dataPeriod = @actualPeriod and 
									eventTime = @lastEntryDate
								)
			end
			else
			begin
			--The first EMA value is the period's SMA
				set @prevEma = (select AVG(closePrice) 
								from #temp_ema
								where rowNumber between 1 and @actualPeriod
								)
				set @emaCalcIndex = @actualPeriod + 1 --Since the first N rows are SMA, we start EMA calc from N + 1 rows
			end
	
			while @emaCalcIndex <= (select COUNT(closePrice) from #temp_ema)
			begin
				set @currentClosePrice = (select closePrice from #temp_ema where rowNumber = @emaCalcIndex)
				set @ema = @smoothingConstant * (@currentClosePrice - @prevEma) + @prevEma
	
				update #temp_ema 
				set ema = @ema 
				where rowNumber = @emaCalcIndex

				set @prevEma = @ema
				set @emaCalcIndex += 1
			end
	
			insert into cry_exponential_moving_average
			select 
				closeTime, 
				@actualSymbol actualSymbol, 
				@actualTimeFrame actualTimeFrame, 
				ema, 
				@actualPeriod actualPeriod
			from #temp_ema

			if @symbolId is not null
			begin
				if @actualPeriod = 50
				set @ema50Out = (select ema from #temp_ema)

				if @actualPeriod = 9
				set @ema9Out = (select ema from #temp_ema)

				if @actualPeriod = 200
				set @ema200Out = (select ema from #temp_ema)

				if @actualPeriod = 10
				set @ema10Out = (select ema from #temp_ema)

				if @actualPeriod = 12
				set @ema12Out = (select ema from #temp_ema)

				if @actualPeriod = 26
				set @ema26Out = (select ema from #temp_ema)
			end

			drop table #temp_ema
			set @periodIndex += 1
		end

		if @symbolId is not null
		begin
			exec MovingAverageConvergenceDivergence
				@symbolIdArg = @actualSymbol, 
				@timeFrameIdArg = @actualTimeFrame,
				@macd1226Out = @macd1226Out output
		end
		else
		begin
			exec MovingAverageConvergenceDivergence
		end

		
		set @symbolIndex+= 1
	end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;