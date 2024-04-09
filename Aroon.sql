create or alter procedure Aroon 
	@symbolId int = NULL, 
	@timeFrameId tinyint = NULL,
	@aroonUp14Out decimal(19,8) = NULL output,
	@aroonDown14Out decimal(19,8) = NULL output,
	@aroonUp25Out decimal(19,8) = NULL output,
	@aroonDown25Out decimal(19,8) = NULL output,
	@debug bit = 0
as
begin
	set nocount on;
	set xact_abort on;
	
	drop table if exists #temp_aroon;
	begin try
	declare @actualPeriod as smallint,
			@periodCount as smallint = 1,
			@symbolCount as int,
			@actualSymbol as int,
			@symbolIndex as int = 1,
			@actualTimeFrame as int,
			@lastEntryDate as datetime2(0)
	
	
	set @periodCount = (select COUNT(periodNumber) from itvf_TAPeriods('Aroon'))
	set @symbolCount = (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
	
	if @debug = 1
	begin
		print concat('Number of periods: ', @periodCount);
		print concat('Number of symbols: ', @symbolCount);
	end
	
	while @symbolIndex <= @symbolCount
	begin
		set @actualSymbol = (select symbolId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
		set @actualTimeFrame = (select timeFrameId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
	
		declare @periodIndex as smallint = 1
	
		while @periodIndex <= @periodCount
		begin
			set @actualPeriod = (select periodNumber from itvf_TAPeriods('Aroon') where rowNumber = @periodIndex)
			set @lastEntryDate = (select
									MAX(eventTime)
								  from cry_aroon 
								  where symbolId = @actualSymbol 
									and timeFrameId = @actualTimeFrame 
									and dataPeriod = @actualPeriod
								  )
	
	
			
			create table #temp_aroon(
				highPrice decimal(19,8),
				lowPrice decimal(19,8),
				closeTime datetime2(0),
				symbol nvarchar(20),
				timeFrame nvarchar(4),
				aroonUp decimal(19,8) null,
				aroonDown decimal(19,8) null,
				rowNumber int null,
			)

			create table #temp_aroon2(
				highPrice decimal(19,8),
				lowPrice decimal(19,8),
				closeTime datetime2(0),
				symbol nvarchar(20),
				timeFrame nvarchar(4),
				aroonUp decimal(19,8) null,
				aroonDown decimal(19,8) null,
			)
			
			CREATE CLUSTERED INDEX temp_aroon_rownumber
			ON #temp_aroon (rowNumber); 
	
				if @debug = 1
				begin
					print concat('Processing symbol: ', @actualSymbol);
					print concat('Processing timeframe: ', @actualTimeFrame);
					print concat('Processing period: ', @actualPeriod)
					print concat('Last entry date in cry_aroon table: ', @lastEntryDate);
				end
	
	
			if (@lastEntryDate is NULL)
			begin
				if @debug = 1
				begin
					print('No previous record for symbol and timeframe in cry_aroon...using full table');
				end
	
				insert into #temp_aroon (highPrice, lowPrice, closeTime, symbol, timeFrame, rowNumber)
				select highPrice, lowPrice, closeTime, symbolId, timeFrameId, ROW_NUMBER() over(order by closeTime) 
				from cry_klines
				where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame
				order by closeTime
			end
			else
			begin
				if @debug = 1
				begin
					print('Previous record found for symbol and timeframe in cry_aroon...using partial data');
				end
	
				insert into #temp_aroon2 (highPrice, lowPrice, closeTime, symbol, timeFrame)
				select highPrice, lowPrice, closeTime, symbolId, timeFrameId
				from cry_klines
				where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame and closeTime >= @lastEntryDate
				order by closeTime desc

				if @debug = 1
				begin
					select * from #temp_aroon
				end

				insert into #temp_aroon2 (highPrice, lowPrice, closeTime, symbol, timeFrame)
				select top (@actualPeriod - 1) highPrice, lowPrice, closeTime, symbolId, timeFrameId
				from cry_klines
				where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame and closeTime < @lastEntryDate
				order by closeTime desc

				insert into #temp_aroon
				select *
					,ROW_NUMBER() over(order by closeTime)
				from #temp_aroon2

			end
	
			declare @firstRow as int = 1,
					@lastRow as int = @actualPeriod + 1,
					@highPriceRow as int,
					@lowPriceRow as int,
					@elapsedPeriodHigh as decimal(19,8),
					@elapsedPeriodLow as decimal(19,8),
					@aroonUp as decimal(19,8),
					@aroonDown as decimal(19,8),
					@correction as bit = 1 --We need this so the last data will be period 0

			if @debug = 1
			begin
				select * from #temp_aroon order by closeTime desc
			end
			
			while @lastRow <= (select COUNT(rowNumber) from #temp_aroon)
			begin
				--Close time must be ordered descendig if we get same values pick the latest
				set @highPriceRow = (select top 1 rowNumber 
									 from #temp_aroon 
									 where highPrice = (select MAX(highPrice) from #temp_aroon where rowNumber between @firstRow and @lastRow)
										and rowNumber between @firstRow and @lastRow
									 order by closeTime desc)
				set @lowPriceRow = (select top 1 rowNumber 
									from #temp_aroon 
									where lowPrice = (select MIN(lowPrice) from #temp_aroon where rowNumber between @firstRow and @lastRow) 
										and rowNumber between @firstRow and @lastRow
									order by closeTime desc)
				set @elapsedPeriodHigh = (select COUNT(rowNumber) from #temp_aroon where rowNumber between @highPriceRow and @lastRow) - @correction
				set @elapsedPeriodLow = (select COUNT(rowNumber) from #temp_aroon where rowNumber between @lowPriceRow and @lastRow) - @correction
				set @aroonDown = ((@actualPeriod - @elapsedPeriodLow) / @actualPeriod) * 100
				set @aroonUp = ((@actualPeriod - @elapsedPeriodHigh) / @actualPeriod) * 100
				
				update #temp_aroon
				set aroonDown = @aroonDown, aroonUp = @aroonUp
				where rowNumber = @lastRow




	
				if @debug = 1
				begin
					print concat('First row: ', @firstRow);
					print concat('Last row: ', @lastRow);
					print concat('Row number of the highest price: ', @highPriceRow);
					print concat('Row number of the lowest price: ', @lowPriceRow);
					print concat('Elapsed period for high price: ', @elapsedPeriodHigh);
					print concat('Elapsed period for low price: ', @elapsedPeriodLow);
					print concat('Aroon up value: ', @aroonUp);
					print concat('Aroon down value: ', @aroonDown);
				end
			
				set @firstRow += 1
				set @lastRow += 1
			end
	
			if (@lastEntryDate is not NULL)
			begin
				delete from #temp_aroon
				where closeTime <= @lastEntryDate
			end
	
			insert into cry_aroon (eventTime, aroonUp, aroonDown, symbolId, timeFrameId, dataPeriod)
			select closeTime, aroonUp, aroonDown, @actualSymbol, @actualTimeFrame, @actualPeriod
			from #temp_aroon
	
			if @debug = 1
				begin
					select * from #temp_aroon order by closeTime
				end
	
			if @actualPeriod = 14 and @symbolId is not null
			begin
				select @aroonUp14Out = aroonUp
					,@aroonDown14Out = aroonDown
				from #temp_aroon
			end
			else
			begin
				select
					@aroonUp25Out = aroonUp,
					@aroonDown25Out = aroonDown
				from #temp_aroon
			end

			drop table #temp_aroon;
			drop table #temp_aroon2;
			set @periodIndex += 1

		end
		set @symbolIndex += 1
	end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;