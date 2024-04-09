create or alter procedure StochasticOscillator 
	@symbolId int = null, 
	@timeFrameId tinyint = null,
	@stoFastOut decimal(19,8) = null output,
	@stoFastSmoothOut decimal(19,8) = null output,
	@stoSlowSmoothOut decimal(19,8) = null output
as
begin

begin try
set nocount on;
set xact_abort on;
	
	if (select count(*) from cry_stochastic_oscillator) = 0
	begin
		with Layer1 as(
		select closeTime, highPrice, lowPrice, closePrice, symbolId, timeFrameId,
			MAX(highPrice) over (partition by symbolId, timeFrameId order by closeTime ROWS BETWEEN 13 PRECEDING AND current ROW) highestHigh,
			MIN(lowPrice) over (partition by symbolId, timeFrameId order by closeTime ROWS BETWEEN 13 PRECEDING AND current ROW) lowestLow,
			ROW_NUMBER() over(partition by symbolId, timeFrameId order by closeTime) rowNumber
		from cry_klines
		),
		Layer2 as (
		select 
			rowNumber, 
			closeTime, 
			symbolId, 
			timeFrameId,
			case
				when closePrice - lowestLow = 0 then 0
				when highestHigh - lowestLow = 0 then 0
				else (closePrice - lowestLow) / (highestHigh - lowestLow) * 100
			end fastSto
		from Layer1
		where rowNumber >= 14
		),
		Layer3 as (
		select *, AVG(fastSto) over (partition by symbolId, timeFrameId order by closeTime rows between 2 preceding and current row) fastSmoothedSto
		from Layer2
		),
		Layer4 as (
		select *, AVG(fastSmoothedSto) over (partition by symbolId, timeFrameId order by closeTime rows between 2 preceding and current row) slowSmoothedSto
		from Layer3
		)
		
		insert into cry_stochastic_oscillator
		select 
			closeTime,
			symbolId, 
			timeFrameId, 
			fastSto, 
			fastSmoothedSto, 
			slowSmoothedSto 
		from Layer4;
	end
	else
	begin		
		declare @actualSymbol as int,
				@symbolIndex as int = 1,
				@actualTimeFrame as tinyint,
				@lastEntryDate as datetime2(0)


		
		while @symbolIndex <= (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
		begin
		create table #temp_sto (
			eventTime datetime2(0),
			symbolId int,
			timeFrameId int,
			fastSto decimal(19,8),
			fastStoSmo decimal(19,8),
			slowStoSmo decimal(19,8)
		)
			set @actualSymbol = (select symbolId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
			set @actualTimeFrame = (select timeFrameId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
			set @lastEntryDate = (
				select max(eventTime)
				from cry_stochastic_oscillator 
				where 
					symbolId = @actualSymbol 
					and timeFrameId = @actualTimeFrame 
				);
			
			with Layer1 as(
			select closeTime, symbolId, timeFrameId, (closePrice - lowestLow) / (highestHigh - lowestLow) * 100 fastSto 
			from (
				select 
					closeTime, 
					highPrice, 
					lowPrice, 
					closePrice, 
					symbolId, 
					timeFrameId,
					MAX(highPrice) over (order by closeTime ROWS BETWEEN 13 PRECEDING AND current ROW) highestHigh,
					MIN(lowPrice) over (order by closeTime ROWS BETWEEN 13 PRECEDING AND current ROW) lowestLow,
					ROW_NUMBER() over(order by closeTime) rowNumber
				from cry_klines
				where 
					closeTime > ISNULL(dbo.scr_PeriodToTime(@actualTimeFrame, 17, @lastEntryDate, 0),'1000-01-01') 
					and symbolId = @actualSymbol 
					and timeFrameId = @actualTimeFrame
			) base
			where rowNumber >= 14
			),
			Layer2 as (
			select *, AVG(fastSto) over (partition by symbolId, timeFrameId order by closeTime rows between 2 preceding and current row) fastSmoothedSto
			from Layer1
			),
			Layer3 as (
			select *, AVG(fastSmoothedSto) over (partition by symbolId, timeFrameId order by closeTime rows between 2 preceding and current row) slowSmoothedSto
			from Layer2
			)

			insert into #temp_sto
			select * 
			from Layer3
			where closeTime > @lastEntryDate


			if @symbolId is not null
			begin
				set @stoFastOut = (select fastSto from #temp_sto)
				set @stoFastSmoothOut = (select fastStoSmo from #temp_sto)
				set @stoSlowSmoothOut = (select slowStoSmo from #temp_sto)
			end

			insert into cry_stochastic_oscillator
			select *
			from #temp_sto

			drop table if exists #temp_sto

			set @symbolIndex+= 1
		end
	end
end try
begin catch
	exec LogErrorMessage;
	throw;
end catch
end;

