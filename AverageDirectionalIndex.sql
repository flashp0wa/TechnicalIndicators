create or alter procedure AverageDirectionalIndex 
	@symbolId int = NULL, 
	@timeFrameId tinyint = NULL,
	@adxOut decimal(4,2) = NULL output
as
begin
	set nocount on;
	set xact_abort on;
	
	drop table if exists #temp_ad_base;
	
	begin try
	declare @symbolIndex as int = 1,
			@actualSymbol as int,
			@actualTimeFrame as tinyint
	
	
	while @symbolIndex <= (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
	begin
		declare @lastEvent as datetime2(0),
				@trIndex as int = 1,
				@currentHigh as decimal(19,8),
				@currentLow as decimal(19,8),
				@previousClose as decimal(19,8),
				@tr1 as decimal(19,8),
				@tr2 as decimal(19,8),
				@tr3 as decimal(19,8),
				@tempTableRows as int,
				@tr14Index as int = 15,
				@tr14 as decimal(19,8),
				@plusDM14 as decimal(19,8),
				@minusDM14 as decimal(19,8),
				@prevTr14 as decimal(19,8),
				@prevPlusDM14 as decimal(19,8),
				@prevMinusDM14 as decimal(19,8),
				@currentPlusDM as decimal(19,8),
				@currentMinusDM as decimal(19,8),
				@currentTr as decimal(19,8),
				@plusDI as decimal(19,8),
				@minusDI as decimal(19,8),
				@DX as decimal(19,8),
				@adxIndex as int = 28,
				@ADX as decimal(19,8),
				@prevADX as decimal(19,8),
				@currentADX as decimal(19,8)
	
		create table #temp_ad_base (
			eventTime datetime2(0) null,
			highPrice decimal(19, 8) null,
			lowPrice decimal(19, 8) null,
			closePrice decimal(19, 8) null,
			plusDM decimal(19, 8) null,
			minusDM decimal(19, 8) null,
			tr decimal(19, 8) null,
			tr14 decimal(19, 8) null,
			plusDM14 decimal(19, 8) null,
			minusDM14 decimal(19, 8) null,
			plusDI14 decimal(19, 8) null,
			minusDI14 decimal(19, 8) null,
			DX decimal(19, 8) null,
			ADX decimal(19, 8) null,
			rowNumber int null 
		)
		
		
		set @actualSymbol = (select symbolId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
		set @actualTimeFrame = (select timeFrameId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
		set @lastEvent = (
							select max(eventTime) 
							from cry_average_directional_index 
							where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame
							)
	
		insert into #temp_ad_base (eventTime, highPrice, lowPrice, closePrice, plusDM, minusDM, rowNumber)
		select closeTime, highPrice, lowPrice, closePrice,
			case
				when highPrice - LAG(highPrice) over (order by closeTime) < 0 then 0
				when highPrice - LAG(highPrice) over (order by closeTime) > LAG(lowPrice) over (order by closeTime) - lowPrice then highPrice - LAG(highPrice) over (order by closeTime) else 0
			end plusDM,
			case
				when LAG(lowPrice) over (order by closeTime) - lowPrice < 0 then 0
				when LAG(lowPrice) over (order by closeTime) - lowPrice > highPrice - LAG(highPrice) over (order by closeTime) then LAG(lowPrice) over (order by closeTime) - lowPrice else 0
			end minusDM,
			ROW_NUMBER() over (order by closeTime) rowNumber
		from cry_klines
		where closeTime >= ISNULL(@lastEvent, '1000-01-01') and symbolId = @actualSymbol and timeFrameId = @actualTimeFrame
		
		set @tempTableRows = (select COUNT(rowNumber) from #temp_ad_base) 
		
		while @trIndex <= @tempTableRows
		begin
			set @currentHigh = (select highPrice from #temp_ad_base where rowNumber = @trIndex)
			set @currentLow = (select lowPrice from #temp_ad_base where rowNumber = @trIndex)
			set @previousClose = ISNULL((select closePrice from #temp_ad_base where rowNumber = @trIndex - 1),0)
			set @tr1 = ABS(@currentHigh - @currentLow)
			set @tr2 = ABS(@currentHigh - @previousClose)
			set @tr3 = ABS(@currentLow - @previousClose)
		
		
			update #temp_ad_base 
			set tr = (select MAX(tr) from (values (@tr1), (@tr2), (@tr3)) as AllValues(tr))
			where rowNumber = @trIndex
		
			set @trIndex += 1
		end
		
		--If there is no previous data start processing from the 2nd row of the temp table
		if (@lastEvent is not NULL)
		begin
			set @tr14Index = 2
			set @adxIndex = 2
			set @prevTr14 = (select top 1 tr14 from cry_average_directional_index where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame order by eventTime desc)
			set @prevMinusDM14 = (select top 1 minusDM14 from cry_average_directional_index where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame order by eventTime desc)
			set @prevPlusDM14 = (select top 1 plusDM14 from cry_average_directional_index where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame order by eventTime desc)
			set @prevADX = (select top 1 adx from cry_average_directional_index where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame order by eventTime desc)
		
			update #temp_ad_base
			set tr14 = @prevTr14, minusDM14 = @prevMinusDM14, plusDM14 = @prevPlusDM14, ADX = @prevADX
			where rowNumber = 1
		end
		
		
		while @tr14Index <= @tempTableRows
		begin
			set @prevTr14 = ISNULL((select tr14 from #temp_ad_base where rowNumber = @tr14Index - 1), 0)
			set @prevMinusDM14 = ISNULL((select minusDM14 from #temp_ad_base where rowNumber = @tr14Index -1), 0)
			set @prevPlusDM14 = ISNULL((select plusDM14 from #temp_ad_base where rowNumber = @tr14Index -1), 0)
			set @currentTr = (select tr from #temp_ad_base where rowNumber = @tr14Index)
			set @currentMinusDM = (select minusDM from #temp_ad_base where rowNumber = @tr14Index)
			set @currentPlusDM = (select plusDM from #temp_ad_base where rowNumber = @tr14Index)
			set @tr14 = case
							when @tr14Index = 15 and @lastEvent is NULL then (select SUM(tr) from #temp_ad_base where rowNumber between 2 and @tr14Index)
							else @prevTr14 - (@prevTr14 / 14) + @currentTr
						end
		
			set @plusDM14 = case
								when @tr14Index = 15 and @lastEvent is NULL then (select SUM(plusDM) from #temp_ad_base where rowNumber between 2 and @tr14Index)
								else @prevPlusDM14 - (@prevPlusDM14 / 14) + @currentPlusDM
							end
		
			set @minusDM14 = case
								when @tr14Index = 15 and @lastEvent is NULL then (select SUM(minusDM) from #temp_ad_base where rowNumber between 2 and @tr14Index)
								else @prevMinusDM14 - (@prevMinusDM14 / 14) + @currentMinusDM
							 end
		
			set @plusDI = (@plusDM14 / @tr14) * 100
			set @minusDI = (@minusDM14 / @tr14) * 100
			if @plusDI - @minusDI = 0 or @plusDI + @minusDI = 0
			begin
				set @DX = 0
			end
			else
			begin
				set @DX = ABS(((@plusDI - @minusDI) / (@plusDI + @minusDI)) * 100)
			end
			update #temp_ad_base
			set tr14 = @tr14, minusDM14 = @minusDM14, plusDM14 = @plusDM14, plusDI14 = @plusDI, minusDI14 = @minusDI, DX = @DX
			where rowNumber = @tr14Index
		
			set @tr14Index += 1
		end
		
		while @adxIndex <= @tempTableRows
		begin
			set @currentADX = (select DX from #temp_ad_base where rowNumber = @adxIndex)
			set @prevADX = (select ADX from #temp_ad_base where rowNumber = @adxIndex - 1)
			set @ADX = case
							when @adxIndex = 28 and @lastEvent is NULL then (select AVG(DX) from #temp_ad_base where rowNumber between 15 and @adxIndex)
							else ((@prevADX * 13) + @currentADX) / 14
						end
		
			update #temp_ad_base
			set ADX = @ADX
			where rowNumber = @adxIndex
		
			set @adxIndex += 1
		end
		
		if (@lastEvent is not NULL)
		begin
			delete from #temp_ad_base where eventTime = @lastEvent
		end
		
		insert into cry_average_directional_index (eventTime, tr14, plusDM14, minusDM14, adx, symbolId, timeFrameId, crossover, crossoverSwitch)
		select
				eventTime,
				tr14,
				plusDM14,
				minusDM14,
				adx,
				@actualSymbol,
				@actualTimeFrame,
				crossover,
				crossoverSwitch = case when LAG(crossover) over(order by eventTime) != crossover  then 1 else 0 end
			from (
			select *,
				crossover = case
								when plusDM14 > minusDM14 then 1
								when plusDM14 < minusDM14 then 2
								else 3
							end
			from(
				select eventTime, tr14, plusDM14, minusDM14, ADX
				from #temp_ad_base
			) as base
		) as layer1;
	
	if @symbolId is not null
	begin
		set @adxOut = (select adx from #temp_ad_base)
	end

	set @symbolIndex += 1
	drop table #temp_ad_base
	end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;