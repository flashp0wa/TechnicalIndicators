create or alter procedure SimpleMovingAverageCrossover @debug bit = 0
as
begin
	begin try

		drop table if exists #mergesma

		create table #mergesma (
		eventTime datetime2(0),
		symbolId int,
		timeFrameId tinyint,
		dataPeriod tinyint,
		crossover1050 tinyint,
		crossover10200 tinyint,
		coSwitch1050 bit null,
		coSwitch10200 bit null
		)
		
		insert into #mergesma
		select *,
			coSwitch1050 = case
							when crossover1050 != LAG(crossover1050) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
						   end,
			coSwitch10200 = case
							when crossover10200 != LAG(crossover10200) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
						   end
		from(
		select
			s10.eventTime,
			s10.symbolId,
			s10.timeFrameId,
			s10.dataPeriod,
			case 
				when s10.sma > s50.sma then (select trendId from cry_trends where trendName = 'bullish')
				when s10.sma < s50.sma then (select trendId from cry_trends where trendName = 'bearish')
				else 3
			end crossover1050,
			case 
				when s10.sma > s200.sma then (select trendId from cry_trends where trendName = 'bullish')
				when s10.sma < s200.sma then (select trendId from cry_trends where trendName = 'bearish')
				else 3
			end crossover10200
		from cry_simple_moving_average s10
		join cry_simple_moving_average s50
			on s10.eventTime = s50.eventTime and
			   s10.symbolId = s50.symbolId and
			   s10.timeFrameId = s50.timeFrameId
		join cry_simple_moving_average s200
			on s10.eventTime = s200.eventTime and
			   s10.symbolId = s200.symbolId and
			   s10.timeFrameId = s200.timeFrameId
		where s10.dataPeriod = 10 and s50.dataPeriod = 50 and s200.dataPeriod = 200 and
			  s10.crossover1050 = 3 and s50.crossover1050 = 3 and s200.crossover1050 = 3
		) base;

		if @debug = 1
		begin
			select count(*) from #mergesma
		end;
		
		
		merge cry_simple_moving_average as trgt
		using #mergesma as src
		on trgt.eventTime = src.eventTime and 
			trgt.symbolId = src.symbolId and 
			trgt.timeFrameId = src.timeFrameId and
			trgt.dataPeriod = src.dataPeriod

		when matched then update set
		trgt.crossover1050 = src.crossover1050,
		trgt.crossover10200 = src.crossover10200,
		trgt.coSwitch1050 = src.coSwitch1050,
		trgt.coSwitch10200 = src.coSwitch10200
		;
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;

