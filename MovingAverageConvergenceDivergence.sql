create or alter procedure MovingAverageConvergenceDivergence 
	@symbolIdArg int = NULL, 
	@timeFrameIdArg int = NULL,
	@macd1226Out decimal(19,8) = NULL output
as
begin
	set xact_abort on;
	set nocount on;
	begin try
		drop table if exists #temp_macd;
		drop table if exists #temp_symbols;

		create table #temp_macd (
			eventTime datetime2(0),
			symbolId int,
			timeFrameId int,
			DIF1226 decimal(19,8),
		);
		create table #temp_symbols (
			symbolId int,
			timeFrameId int,
			rowNumber int
		)
		
		declare		@actualSymbol as int,
					@symbolIndex as int = 1,
					@actualTimeFrame as tinyint,
					@lastEntryDate as datetime2(0),
					@deaEma as decimal(19,8),
					@prevDeaEma as decimal(19,8),
					@smoothingValue as tinyint = 2,
					@smoothingConstant decimal(5,4),
					@symbolId int,
					@timeFrameId int
		

		if @symbolIdArg is null and @timeFrameIdArg is null
		begin
			set @lastEntryDate = (
						 select max(eventTime)
						 from cry_moving_average_convergence_divergence					 
						 )
			insert into #temp_macd
			select 
				d9.eventTime, 
				d9.symbolId,
				d9.timeFrameId,
				d12.ema - d26.ema DIF1226
			from 
				cry_exponential_moving_average d9 
			join 
				cry_exponential_moving_average d12 
			on 
				d9.eventTime = d12.eventTime and 
				d9.symbolId = d12.symbolId and
				d9.timeFrameId = d12.timeFrameId
			join 
				cry_exponential_moving_average d26
			on 
				d9.eventTime = d26.eventTime and 
				d9.symbolId = d26.symbolId and
				d9.timeFrameId = d26.timeFrameId
			where 
				d9.dataPeriod = 9 and
				d12.dataPeriod = 12 and
				d26.dataPeriod = 26 and
				d9.ema is not null and 
				d12.ema is not null and 
				d26.ema is not null
				and d9.eventTime > ISNULL(@lastEntryDate, '1000-01-01')
		end
		else
		begin
			set @lastEntryDate = (
				 select MAX(eventTime)
				 from cry_moving_average_convergence_divergence
				 where symbolId = @symbolIdArg and timeFrameId = @timeFrameIdArg
				 )

			insert into #temp_macd
			select 
				d9.eventTime, 
				d9.symbolId,
				d9.timeFrameId,
				d12.ema - d26.ema DIF1226
			from 
				cry_exponential_moving_average d9 
			join 
				cry_exponential_moving_average d12 
			on 
				d9.eventTime = d12.eventTime and 
				d9.symbolId = d12.symbolId and
				d9.timeFrameId = d12.timeFrameId
			join 
				cry_exponential_moving_average d26
			on 
				d9.eventTime = d26.eventTime and 
				d9.symbolId = d26.symbolId and
				d9.timeFrameId = d26.timeFrameId
			where 
				d9.dataPeriod = 9 and
				d12.dataPeriod = 12 and
				d26.dataPeriod = 26 and
				d9.ema is not null and 
				d12.ema is not null and 
				d26.ema is not null
				and d9.symbolId = @symbolIdArg
				and d9.timeFrameId = @timeFrameIdArg
				and d9.eventTime > ISNULL(@lastEntryDate, '1000-01-01')
		end

		insert into #temp_symbols
		select
			symbolId,
			timeFrameId,
			ROW_NUMBER() over(order by symbolId) rowNumber
		from #temp_macd
		group by symbolId, timeFrameId

		while @symbolIndex <= (select COUNT(rowNumber) from #temp_symbols)
		begin
			drop table if exists #temp_macd_singles;
			create table #temp_macd_singles (
				eventTime datetime2(0),
				symbolId int,
				timeFrameId int,
				DIF1226 decimal(19,8),
				DEA1226 decimal(19,8),
				rowNumber int
			);
		
			set @actualSymbol = (select symbolId 
								 from #temp_symbols 
								 where rowNumber = @symbolIndex
								 )
		
			set @actualTimeFrame = (select timeFrameId 
									from #temp_symbols 
									where rowNumber = @symbolIndex
								 )
		
			set @smoothingConstant = cast(@smoothingValue as decimal(5,4)) / cast((10) as decimal(8,4))
		
			declare @currentDif as decimal(19,8),
					@emaCalcIndex as int = 1
		
			insert into #temp_macd_singles (
				eventTime,
				symbolId,
				timeFrameId,
				DIF1226,
				rowNumber)
			select *,
				ROW_NUMBER() over(order by eventTime) rowNumber
			from #temp_macd
			where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame
		
		
			if @lastEntryDate is not null
			begin
				set @prevDeaEma = (select DEA1226 
								from cry_moving_average_convergence_divergence
								where symbolId = @actualSymbol and 
									timeFrameId = @actualTimeFrame and
									eventTime = @lastEntryDate
								);
			end
			else
			begin
			--The first EMA value is the period's SMA
				set @prevDeaEma = (select AVG(DIF1226) 
								from #temp_macd_singles
								where rowNumber between 1 and 9
								)
				set @emaCalcIndex = 10 --Since the first N rows are SMA, we start EMA calc from N + 1 rows
			end
			
			while @emaCalcIndex <= (select COUNT(DIF1226) from #temp_macd_singles)
			begin
				set @currentDif = (select DIF1226 from #temp_macd_singles where rowNumber = @emaCalcIndex)
				set @deaEma = @smoothingConstant * (@currentDif - @prevDeaEma) + @prevDeaEma
			
				update #temp_macd_singles 
				set DEA1226 = @deaEma 
				where rowNumber = @emaCalcIndex
			
				set @prevDeaEma = @deaEma
				set @emaCalcIndex += 1

			end
		
			insert into cry_moving_average_convergence_divergence
			select 
				eventTime,
				symbolId,
				timeFrameId,
				DIF1226,
				DEA1226,
				DIF1226 - DEA1226
			from #temp_macd_singles

			if @symbolIdArg is not null
			begin
				set @macd1226Out = (select dif1226 - dea1226 from #temp_macd_singles)
			end
			
			set @symbolIndex += 1;
		end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;