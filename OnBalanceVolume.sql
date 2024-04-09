create or alter procedure OnBalanceVolume 
	@symbolId int = NULL, 
	@timeFrameId tinyint = NULL,
	@obvOut decimal(19,8) = NULL OUTPUT
as
begin
	begin try
	set nocount on;
	set xact_abort on;

	declare @actualSymbol as int,
			@symbolIndex as int = 1,
			@obvIndex as int = 1,
			@prevObv as decimal(29,8) = 0,
			@obv as decimal(29,8),
			@currentClosePrice as decimal(19,8),
			@currentVolume as decimal(19,8),
			@prevClosePrice as decimal(19,8) = 0,
			@actualTimeFrame as tinyint,
			@lastEntryDate as datetime2(0)


	while @symbolIndex <= (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))
	begin
		set @actualSymbol = (select symbolId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
		set @actualTimeFrame = (select timeFrameId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
		set @lastEntryDate = (
			select max(eventTime) 
			from cry_on_balance_volume 
			where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame
			)

		create table #temp_obv(
			closeTime datetime2(0),
			closePrice decimal(19,8),
			volume decimal(19,8),
			obv decimal(29,8) null,
			rowNumber int primary key
		)


		insert into #temp_obv (closeTime, closePrice, volume, rowNumber)
		select closeTime, closePrice, quoteAssetVolume, ROW_NUMBER() over (order by closeTime)
		from cry_klines
		where 
			symbolId = @actualSymbol 
			and timeFrameId = @actualTimeFrame 
			and closeTime >= ISNULL(@lastEntryDate, '1000-01-01')

		if @lastEntryDate is not null
		begin
			set @prevObv = (
				select onBalanceVolume
				from cry_on_balance_volume
				where 
					symbolId = @actualSymbol 
					and timeFrameId = @actualTimeFrame 
					and eventTime = @lastEntryDate)
		end

		while @obvIndex <= (select COUNT(*) from #temp_obv)
		begin
			if @obvIndex = 1
			begin
				set @prevClosePrice = (select closePrice from #temp_obv where rowNumber = @obvIndex)
				set @obvIndex += 1
				continue;
			end

			set @currentClosePrice = (select closePrice from #temp_obv where rowNumber = @obvIndex)
			set @currentVolume = (select volume from #temp_obv where rowNumber = @obvIndex)
			set @obv = case
						when @currentClosePrice > @prevClosePrice then @prevObv + @currentVolume
						when @currentClosePrice < @prevClosePrice then @prevObv - @currentVolume
						when @currentClosePrice = @prevClosePrice then @prevObv + 0
					   end

			update #temp_obv set obv = @obv where rowNumber = @obvIndex

			set @prevObv = @obv
			set @prevClosePrice = @currentClosePrice
			set @obvIndex += 1
		end
		delete from #temp_obv where rowNumber = 1

		insert into cry_on_balance_volume
		select closeTime, @actualSymbol, @actualTimeFrame, obv
		from #temp_obv

		if @symbolId is not null
		begin
			set @obvOut = (select obv from #temp_obv)
		end

		drop table #temp_obv

		set @prevObv = 0
		set @obvIndex = 1
		set @prevClosePrice = 0
		set @symbolIndex+= 1
	end
	end try
	begin catch
		exec LogErrorMessage;
		throw;
	end catch
end;