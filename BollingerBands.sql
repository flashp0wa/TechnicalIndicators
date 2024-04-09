create or alter procedure BollingerBands 
	@symbolId int = null, 
	@timeFrameId smallint = null,
	@upper30Out decimal(19,8) = null output,
	@lower30Out decimal(19,8) = null output,
	@width30Out decimal(19,8) = null output
as
	begin
	begin try

		declare @dataPeriod smallint = 30; -- If you change this do not forget to change standard deviation's over clause

		if (select count(*) from cry_bollinger_bands) = 0
		begin
			with l1 as (
				select
					kl.closeTime
					,kl.closePrice
					,kl.symbolId
					,kl.timeFrameId
					,sma.sma
					,stdev(kl.closePrice) over(partition by kl.symbolId, kl.timeFrameId ORDER BY kl.closeTime ROWS BETWEEN 29 PRECEDING AND current ROW) deviation
				from cry_klines kl
				join cry_simple_moving_average sma
					on kl.symbolId = sma.symbolId
						and kl.timeFrameId = sma.timeFrameId
						and kl.closeTime = sma.eventTime
				where sma.dataPeriod = @dataPeriod
				),
				l2 as (
					select *
						,sma + deviation * 2 upperBand
						,sma - deviation * 2 lowerBand
					from l1
				),
				l3 as (
					select *
						,upperBand - lowerBand bandWidth
					from l2
				)
				
				insert into cry_bollinger_bands
				select 
					closeTime
					,symbolId
					,timeFrameId
					,upperBand
					,lowerBand
					,isnull(bandWidth,0)
					,@dataPeriod
				from l3
		end
		else
		begin
			declare @actualSymbol as int,
					@symbolIndex as int = 1,
					@actualTimeFrame as tinyint,
					@lastEntryDate as datetime2(0)

			
			while @symbolIndex <= (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
			begin

			create table #temp_boll (
				eventTime datetime2(0),
				symbolId int,
				timeFrameId int,
				upperBand decimal(19,8),
				lowerBand decimal(19,8),
				bandWith decimal(19,8),
				dataPeriod int
			)
				set @actualSymbol = (select symbolId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
				set @actualTimeFrame = (select timeFrameId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
				set @lastEntryDate = (select max(eventTime) from cry_bollinger_bands where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame);
				with l1 as (
					select
						kl.closeTime
						,kl.closePrice
						,kl.symbolId
						,kl.timeFrameId
						,sma.sma
						,stdev(kl.closePrice) over(ORDER BY kl.closeTime ROWS BETWEEN 29 PRECEDING AND current ROW) deviation
						,ROW_NUMBER() over(order by kl.closeTime) rowNum
					from cry_klines kl
					left join cry_simple_moving_average sma
						on kl.symbolId = sma.symbolId
							and kl.timeFrameId = sma.timeFrameId
							and kl.closeTime = sma.eventTime
					where sma.dataPeriod = @dataPeriod
						and kl.symbolId = @actualSymbol
						and kl.timeFrameId = @actualTimeFrame
						and kl.closeTime >= ISNULL(dbo.scr_PeriodToTime(@actualTimeFrame, @dataPeriod, @lastEntryDate, 1), '1000-01-01')
					),
					l2 as (
						select *
							,sma + deviation * 2 upperBand
							,sma - deviation * 2 lowerBand
						from l1
					),
					l3 as (
						select *
							,upperBand - lowerBand bandWidth
						from l2
					)
					
				insert into #temp_boll
				select 
					closeTime
					,symbolId
					,timeFrameId
					,upperBand
					,lowerBand
					,bandWidth
					,@dataPeriod
				from l3
				where rowNum > @dataPeriod
				order by closeTime desc;
					

				insert into cry_bollinger_bands
				select *
				from #temp_boll
			
				if @symbolId is not null
				begin			
					set @upper30Out = (select upperBand from #temp_boll)
					set	@lower30Out = (select lowerBand from #temp_boll)
					set @width30Out = (select bandWith from #temp_boll)
				end
			
				drop table if exists #temp_boll
				
				print @symbolIndex
				set @symbolIndex += 1
			end
		end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;