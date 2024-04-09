create or alter procedure ExponentialMovingAverageCrossover
as
begin
	begin try
		select *,
			coSwitch1050 = case
							when crossover1050 != LAG(crossover1050) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
						   end,
			coSwitch10200 = case
							when crossover10200 != LAG(crossover10200) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
						   end,
			coSwitch912 = case
							when crossover912 != LAG(crossover912) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
						   end,
			coSwitch1226 = case
							when crossover1226 != LAG(crossover1226) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
						   end,
			pcoSwitch50 = case
								when priceCrossover50 != LAG(priceCrossover50) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
							end,
			pcoSwitch200 = case
								when priceCrossover200 != LAG(priceCrossover200) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
							end,
			pcoSwitch9 = case
								when priceCrossover9 != LAG(priceCrossover9) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
							end,
			pcoSwitch12 = case
								when priceCrossover12 != LAG(priceCrossover12) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
							end,
			pcoSwitch26 = case
								when priceCrossover26 != LAG(priceCrossover26) over(order by symbolId, timeFrameId, eventTime) then 1 else 0
							end
		from(
			select
				e10.eventTime,
				e10.symbolId,
				e10.timeFrameId,
				case 
					when e10.ema > e50.ema then (select trendId from cry_trends where trendName = 'bullish')
					when e10.ema < e50.ema then (select trendId from cry_trends where trendName = 'bearish')
					else 3
				end crossover1050,
				case 
					when e10.ema > e200.ema then (select trendId from cry_trends where trendName = 'bullish')
					when e10.ema < e200.ema then (select trendId from cry_trends where trendName = 'bearish')
					else 3
				end crossover10200,
				case 
					when e12.ema > e26.ema then (select trendId from cry_trends where trendName = 'bullish')
					when e12.ema < e26.ema then (select trendId from cry_trends where trendName = 'bearish')
					else 3
				end crossover1226,
				case 
					when e9.ema > e12.ema then (select trendId from cry_trends where trendName = 'bullish')
					when e9.ema < e12.ema then (select trendId from cry_trends where trendName = 'bearish')
					else 3
				end crossover912,
				priceCrossover50 = case
									when e50.ema < kl.closePrice then 1
									when e50.ema > kl.closePrice then 2
									else 3
								end,
				priceCrossover200 = case
									when e200.ema < kl.closePrice then 1
									when e200.ema > kl.closePrice then 2
									else 3
								end,
				priceCrossover12 = case
									when e12.ema < kl.closePrice then 1
									when e12.ema > kl.closePrice then 2
									else 3
								end,
				priceCrossover26 = case
									when e26.ema < kl.closePrice then 1
									when e26.ema > kl.closePrice then 2
									else 3
								end,
				priceCrossover9 = case
									when e9.ema < kl.closePrice then 1
									when e9.ema > kl.closePrice then 2
									else 3
								end
			from cry_exponential_moving_average e10
			join cry_exponential_moving_average e50
				on 
					e10.eventTime = e50.eventTime and
					e10.symbolId = e50.symbolId and
					e10.timeFrameId =e50.timeFrameId
			join cry_exponential_moving_average e200
				on 
					e10.eventTime = e200.eventTime and
					e10.symbolId = e200.symbolId and
					e10.timeFrameId = e200.timeFrameId
			join cry_exponential_moving_average e9
				on 
					e10.eventTime = e9.eventTime and
					e10.symbolId = e9.symbolId and
					e10.timeFrameId =e9.timeFrameId
			join cry_exponential_moving_average e12
				on 
					e10.eventTime = e12.eventTime and
					e10.symbolId = e12.symbolId and
					e10.timeFrameId =e12.timeFrameId
			join cry_exponential_moving_average e26
				on 
					e10.eventTime = e26.eventTime and
					e10.symbolId = e26.symbolId and
					e10.timeFrameId =e26.timeFrameId
			join cry_klines kl
				on 
					e10.eventTime = kl.closeTime
					and e10.symbolId = kl.symbolId
					and e10.timeFrameId = kl.timeFrameId
			where 
				e10.dataPeriod = 10 
				and e50.dataPeriod = 50
				and e200.dataPeriod = 200
				and e9.dataPeriod = 9
				and e12.dataPeriod = 12
				and e26.dataPeriod = 26
		) as base
order by eventTime desc
end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;