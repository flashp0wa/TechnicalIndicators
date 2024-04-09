create or alter procedure AverageTrueRange 
	@symbolId int = null, 
	@timeFrameId smallint = null,
	@atr50Out decimal(19,8) = null output,
	@debug bit = 0
as
	begin
		set nocount on;
		set xact_abort on;
	begin try

		declare @actualSymbol as int,
				@symbolIndex as int = 1,
				@actualTimeFrame as tinyint,
				@lastEntryDate as datetime2(0),
				@dataPeriod smallint = (select periodNumber 
										from cry_technical_analysis_periods 
										where technicalIndicator = 'Average True Range'),
				@rowIndex int,
				@totalRows int,
				@atr decimal(19,8),
				@firstRun bit,
				@messageText varchar(5000)
		
		drop table if exists #temp_atr

		create table #temp_atr (
			eventTime datetime2(0),
			tr decimal(19,8),
			atr decimal(19,8),
			rowNumber int
		)

		create nonclustered index NIX_TempAtr_RowNumber on #temp_atr(rowNumber)
		-- Start looping through kline symbols
		while @symbolIndex <= (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
		begin
			set @actualSymbol = (select symbolId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
			set @actualTimeFrame = (select timeFrameId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
			set @lastEntryDate = (select max(eventTime)
								  from cry_average_true_range 
								  where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame 
								  )
			set @firstRun = 0;

			set @messageText = CONCAT('Calculating symbol: ', @actualSymbol, ' timeframe: ', @actualTimeFrame)

			if @debug = 1
			begin
				print 'actualSymbol:' + cast(@actualSymbol as varchar(6))
				print 'actualTimeFrame:' + cast(@actualTimeFrame as varchar(6))
				print 'lastEntryDate:' + cast(@lastEntryDate as varchar(30))
				print 'firstRun:' + cast(@firstRun as varchar(1))
			end

			-- Is there new kline data?
			if @lastEntryDate = (select top 1 closeTime from cry_klines where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame order by closeTime desc)
			begin
				set @symbolIndex += 1
				continue
			end

			-- Calculate TR values into temp table
			insert into #temp_atr (eventTime, tr, rowNumber)
			select closeTime
				,(select MAX(tr) from (values (sub.hl), (sub.hlc), (sub.llc)) as Maximum(tr)) tr
				,ROW_NUMBER() over(order by closeTime) rowNumber
			from (
			select *
				,highPrice - lowPrice hl
				,abs(highPrice - LAG(closePrice) over(order by closeTime)) hlc
				,abs(lowPrice - LAG(closePrice) over(order by closeTime)) llc
			from cry_klines
			where symbolId = @actualSymbol 
				and timeFrameId = @actualTimeFrame 
				--We need the close price 1 period before the last entrydate
				and closeTime > ISNULL(dbo.scr_PeriodToTime(@actualTimeFrame, 1, @lastEntryDate, 0), '1111-11-11') 
			) sub


			if @debug = 1
			begin
				print 'periodToTime:'
				select dbo.scr_PeriodToTime(@actualSymbol, 1, @lastEntryDate, 0)
				print 'Printing #temp_atr'
				select * from #temp_atr
			end
			
			if @lastEntryDate is null
			begin
				set @rowIndex = @dataPeriod
			end
			else
			begin
				set @rowIndex = 1
			end

			set @totalRows = (select COUNT(*) from #temp_atr)

			while @rowIndex <= @totalRows
			begin

				if @lastEntryDate is null and @firstRun = 0
				begin
					set @atr = (select AVG(tr) from #temp_atr where rowNumber < @rowIndex)
			
					update #temp_atr
					set atr = @atr
					where rowNumber = @rowIndex
					
					set @firstRun = 1
					set @rowIndex += 1
					continue
				end
				if @firstRun = 0
				begin
					set @atr = (select top 1 atr 
								from cry_average_true_range 
								where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame
								order by eventTime desc)
					set @firstRun = 1
				end
			
				set @atr = ((@atr * (@dataPeriod - 1)) + (select tr from #temp_atr where rowNumber = @rowIndex)) / @dataPeriod
			
				update #temp_atr
				set atr = @atr
				where rowNumber = @rowIndex

			
				set @rowIndex += 1
			end
		
			insert into cry_average_true_range
			select eventTime, @actualSymbol, @actualTimeFrame, atr
			from #temp_atr
			where atr is not null

			if @symbolId is not null
			begin
				set @atr50Out = (select atr from #temp_atr)
			end

			delete from #temp_atr
			
			set @symbolIndex += 1
		end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;