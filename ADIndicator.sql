create or alter procedure ADIndicator 
	@symbolId int = NULL, 
	@timeFrameId tinyint = NULL, 
	@adOut decimal(19,2) = NULL OUTPUT,
	@debug bit = 0
as
set nocount on;
set xact_abort on;

drop table if exists #temp_adi_base;

begin try
declare @symbolCount as int,
		@symbolIndex as int = 1,
		@actualSymbol as int,
		@actualTimeFrame as int,
		@calculationCount as int,
		@close as decimal(19,8),
		@low as decimal(19,8),
		@high as decimal(19,8),
		@volume as money,
		@moneyFlowMultiplier as decimal(19,8),
		@moneyFlowVolume as decimal(19,8),
		@prevAd as decimal(19,2) = 0,
		@ad as decimal(19,2),
		@lastEntryDate as datetime2(0)


set @symbolCount = (select COUNT(symbolId) from itvf_Symbols(@symbolId, @timeFrameId))

while @symbolIndex <= @symbolCount
begin
	declare @calculationIndex as int = 1

	create table #temp_adi_base (
	closePrice decimal(19,8),
	lowPrice decimal(19,8),
	highPrice decimal(19,8),
	volume money,
	closeTime datetime2(0),
	rowNumber int,
	adi decimal(20,8) null
	)

	set @actualSymbol = (select symbolId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
	set @actualTimeFrame = (select timeFrameId from itvf_Symbols(@symbolId, @timeFrameId) where rowNumber = @symbolIndex)
	set @prevAd = ISNULL((select top 1 adi from cry_accumulation_distribution_indicator where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame order by eventTime desc), 0)
	


	if (@prevAd != 0)
	begin
		set @lastEntryDate = (select MAX(eventTime) from cry_accumulation_distribution_indicator where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame)
	end

	insert into #temp_adi_base (closePrice, lowPrice, highPrice, volume, closeTime, rowNumber) 
	select closePrice, lowPrice, highPrice, volume, closeTime, ROW_NUMBER() over(order by closeTime) as rowNumber
	from cry_klines 
	where symbolId = @actualSymbol and timeFrameId = @actualTimeFrame and closeTime > case
																					when @prevAd = 0 then '1000-01-01' else @lastEntryDate
																				  end
	if @debug = 1
	begin
		print concat('!!!---Processing symbol: ', @actualSymbol, '---!!!');
		print concat('Processing timeframe: ', @actualTimeFrame);
		print concat('Last entry date in ADIndicator table: ', @lastEntryDate);
	end

	set @calculationCount = (select COUNT(*) from #temp_adi_base)

		while @calculationIndex <= @calculationCount
		begin
			set @close = (select closePrice from #temp_adi_base where rowNumber = @calculationIndex)
			set @low = (select lowPrice from #temp_adi_base where rowNumber = @calculationIndex)
			set @high = (select highPrice from #temp_adi_base where rowNumber = @calculationIndex)

			if @debug = 1
			begin
				print concat('Close price: ', @close)
				print concat('Low price: ', @low)
				print concat('High price: ', @high)
			end
			
			if (@high - @low) = 0
			begin
				set @moneyFlowMultiplier = 0
			end
			else
			begin
				set @moneyFlowMultiplier = ((@close - @low) - (@high - @close)) / (@high - @low)
			end

			set @volume = (select volume from #temp_adi_base where rowNumber = @calculationIndex)
			set @moneyFlowVolume = @moneyFlowMultiplier * cast(@volume as decimal(19,8))			
			set @ad = round(@prevAd + @moneyFlowVolume, 2)

			if @debug = 1
			begin
				print concat('Moneyflow multiplier: ', @moneyFlowMultiplier)
				print concat('Volume: ', @volume)
				print concat('Moneyflow volume: ', @moneyFlowVolume)
				print concat('AD: ', @ad)
				print concat('Previous AD: ', @prevAd)
			end

			update #temp_adi_base
			set adi = @ad
			where rowNumber = @calculationIndex

			set @prevAd = @ad
			set @calculationIndex += 1

		end

	insert into cry_accumulation_distribution_indicator (eventTime, adi, symbolId, timeFrameId)
	select closeTime, adi, @actualSymbol, @actualTimeFrame 
	from #temp_adi_base

	if @symbolId is not null
	begin
		select @adOut = adi
		from #temp_adi_base

		return;
	end


	drop table #temp_adi_base
set @symbolIndex += 1
end
end try
begin catch
	exec LogErrorMessage;
	throw;
end catch