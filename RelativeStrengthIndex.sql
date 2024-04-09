create or alter procedure RelativeStrengthIndex 
	@symbolId int = NULL, 
	@timeFrameId tinyint = NULL,
	@rsiOut decimal(19,8) = NULL output
as
begin
	begin try
	
	set nocount on;
	set xact_abort on;
	
	declare 
			@gainCurrent as decimal(19,8),
			@lossCurrent as decimal(19,8),
			@avgGainPrior as decimal(19,8),
			@avgLossPrior as decimal(19,8),
			@avgGainCurrent as decimal(19,8),
			@avgLossCurrent as decimal(19,8),
			@currentDate as datetime2(0),
			@actualSymbol as int,
			@symbolCount as int,
			@symbolIndex as int = 1,
			@actualTimeFrame as tinyint,
			@lastEntryDate as datetime2(0),
			@sampleRate as int
	
	set @sampleRate = (select periodNumber from cry_technical_analysis_periods where technicalIndicator = 'Relative Strength Index')
	
	drop table if exists #temp_rsi;
	drop table if exists #temp_base; -- Logically easier to use two tables (trust yourself)
	
	set @symbolCount = (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
	
	while @symbolIndex <= @symbolCount
	begin
		declare @rsiIndex as int = 1,
				@baseIndex as int = 16
	
		create table #temp_rsi(
		closeTime datetime2(0),
		avgGain decimal(19,8),
		avgLoss decimal(19,8),
		symbol nvarchar(20),
		timeFrame nvarchar(4),
		rsi decimal(19,8),
		rowNumber int
		)
		
		set @actualSymbol = (select symbolId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
		set @actualTimeFrame = (select timeFrameId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
		set @lastEntryDate = (
			select max(eventTime) 
			from cry_relative_strength_index 
			where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame 
			)
		
		
			select 
				closeTime
				,closePrice
				,symbolId
				,timeFrameId
				,closePrice - lag(closePrice, 1) over(order by closeTime) changeInPrice
				,isnull(case
							when ([closePrice]-(lag([closePrice]) over(order by closeTime))) > 0
								then [closePrice]-(lag([closePrice]) over(order by closeTime))
							when ([closePrice]-(lag([closePrice]) over(order by closeTime))) = 0
								then 0
					end, 0) gain
				,abs(isnull(case
								when ([closePrice]-(lag([closePrice]) over(order by closeTime))) < 0
									then [closePrice]-(lag([closePrice]) over(order by closeTime))
								when ([closePrice]-(lag([closePrice]) over(order by closeTime))) = 0
									then 0
					end, 0)) loss
				,ROW_NUMBER() over (order by closeTime asc) as rowNumber
			into #temp_base
			from cry_klines
			where 
				symbolId = @actualSymbol 
				and timeFrameId = @actualTimeFrame 
				and closeTime >= ISNULL(@lastEntryDate, '1000-01-01')
	
		if (@lastEntryDate is NULL)
		begin
			insert into #temp_rsi (
				closeTime
				,avgGain
				,avgLoss
				,symbol
				,rsi
				,rowNumber
				,timeFrame
				)
			select 
				max(closeTime) closeTime
				,AVG(gain) avgGain
				,avg(loss) avgLoss
				,symbolId
				,case 
					when AVG(loss) = 0 
						then 100 
					else 100 - (100/ (1 + (AVG(gain) / AVG(loss)))) 
				end rsi
				,rowNumber = 1
				,timeFrameId
			from #temp_base
			where 
				rowNumber between 2 and @sampleRate + 1  --first row is always null, we don't need it
			group by 
				symbolId
				,timeFrameId
		end
		else
		begin
			set @baseIndex = 2 --we check from the second row because the first one has no gain or loss
			insert into #temp_rsi (
				closeTime
				,avgGain
				,avgLoss
				,symbol
				,rsi
				,rowNumber
				,timeFrame
				)
			select top 1 
				eventTime
				,avgGain
				,avgLoss
				,symbolId
				,rsi
				,rowNumber = 1
				,timeFrameId 
			from cry_relative_strength_index 
			where 
				symbolId = @actualSymbol 
				and timeFrameId = @actualTimeFrame
			order by eventTime desc
		end
		
			while @baseIndex <= (select count(closeTime) from #temp_base)
			begin
				select
					@avgGainPrior = (select avgGain from #temp_rsi where rowNumber = @rsiIndex),
					@avgLossPrior = (select avgLoss from #temp_rsi where rowNumber = @rsiIndex),
					@gainCurrent = (select gain from #temp_base where rowNumber = @baseIndex),
					@lossCurrent = (select loss from #temp_base where rowNumber = @baseIndex),
					@avgGainCurrent = ((@avgGainPrior * (@sampleRate -1)) + @gainCurrent) / @sampleRate,
					@avgLossCurrent = ((@avgLossPrior * (@sampleRate -1)) + @lossCurrent) / @sampleRate,
					@currentDate = (select closeTime from #temp_base where rowNumber = @baseIndex)
		
				set @rsiIndex += 1
			
				insert into #temp_rsi values (
				@currentDate,
				@avgGainCurrent,
				@avgLossCurrent,
				@actualSymbol,
				@actualTimeFrame,
				case when @avgLossCurrent != 0 then 100 - (100/ (1 + (@avgGainCurrent / @avgLossCurrent))) else 100 end,
				@rsiIndex
				)
		
				set @baseIndex += 1
			end
		
			if (@lastEntryDate is not NULL)
			begin
				delete from #temp_rsi where closeTime <= @lastEntryDate
			end
	
		
			insert into cry_relative_strength_index (
				eventTime
				,avgGain
				,avgLoss
				,symbolId
				,rsi
				,timeFrameId
				)
			select 
				closeTime
				,avgGain
				,avgLoss
				,symbol
				,rsi
				,timeFrame
			from #temp_rsi

			if @symbolId is not null
			begin
				set @rsiOut = (select rsi from #temp_rsi)
			end
	
	set @symbolIndex += 1
	drop table if exists #temp_base;
	drop table if exists #temp_rsi;
	end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;