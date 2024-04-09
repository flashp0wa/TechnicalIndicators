create or alter procedure WilliamsFractal 
	@symbolId int = NULL, 
	@timeFrameId int = NULL,
	@buyOut bit = NULL output,
	@sellOut bit = null output
as
begin
	begin try
		declare @actualSymbol as int,
				@symbolIndex as int = 1,
				@actualTimeFrame as tinyint,
				@lastEntryDate as datetime2(0);


		if (select count(*) from cry_williams_fractal) = 0
		begin
			insert into cry_williams_fractal
			select
				closeTime,
				symbolId,
				timeFrameId,
				case 
					when 
						lag(closePrice,2) over(partition by symbolId, timeFrameId order by closeTime) < closePrice
					and lag(closePrice) over(partition by symbolId, timeFrameId order by closeTime) < closePrice
					and lead(closePrice) over(partition by symbolId, timeFrameId order by closeTime) < closePrice
					and lead(closePrice,2) over(partition by symbolId, timeFrameId order by closeTime) < closePrice
				then 1 else 0 end buyFractal,
				case 
					when 
						lag(closePrice,2) over(partition by symbolId, timeFrameId order by closeTime) > closePrice
					and lag(closePrice) over(partition by symbolId, timeFrameId order by closeTime) > closePrice
					and lead(closePrice) over(partition by symbolId, timeFrameId order by closeTime) > closePrice
					and lead(closePrice,2) over(partition by symbolId, timeFrameId order by closeTime) > closePrice
				then 1 else 0 end sellFractal
			from cry_klines
		end
		else
		begin
			

			while @symbolIndex <= (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
			begin
			create table #temp_willy (
				eventTime datetime2(0),
				symbolId int,
				timeFrameId int,
				buyFractal bit,
				sellFractal bit
			)

				set @actualSymbol = (
					select symbolId 
					from itvf_Symbols(@symbolId, @timeFrameId) 
					where rowNumber = @symbolIndex
					)

				set @actualTimeFrame = (
					select timeFrameId 
					from itvf_Symbols(@symbolId, @timeFrameId) 
					where rowNumber = @symbolIndex
					)

				set @lastEntryDate = (
					select max(eventTime)
					from cry_williams_fractal 
					where symbolId = @actualSymbol and 
						timeFrameId = @actualTimeFrame
					)

					delete 
					from cry_williams_fractal 
					where eventTime >= dbo.scr_PeriodToTime(@actualTimeFrame, 2, @lastEntryDate, 0)
						and symbolId = @actualSymbol
						and timeFrameId = @actualTimeFrame

					insert into #temp_willy
					select
						closeTime,
						symbolId,
						timeFrameId,
						case 
							when 
								lag(closePrice,2) over(order by closeTime) < closePrice
							and lag(closePrice) over(order by closeTime) < closePrice
							and lead(closePrice) over(order by closeTime) < closePrice
							and lead(closePrice,2) over(order by closeTime) < closePrice
						then 1 else 0 end buyFractal,
						case 
							when 
								lag(closePrice,2) over(order by closeTime) > closePrice
							and lag(closePrice) over(order by closeTime) > closePrice
							and lead(closePrice) over(order by closeTime) > closePrice
							and lead(closePrice,2) over(order by closeTime) > closePrice
						then 1 else 0 end sellFractal
					from cry_klines
					where closeTime > dbo.scr_PeriodToTime(@actualTimeFrame, 3, @lastEntryDate, 0)
						and symbolId = @actualSymbol
						and timeFrameId = @actualTimeFrame
					
					insert into cry_williams_fractal
					select *
					from #temp_willy


					if @symbolId is not null
					begin
						set @buyOut = (select top 1 buyFractal from #temp_willy order by eventTime desc)
						set @sellOut = (select top 1 sellFractal from #temp_willy order by eventTime desc)
					end

					drop table if exists #temp_willy
	
				set @symbolIndex += 1
			end
		end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;