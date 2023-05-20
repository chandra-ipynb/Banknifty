/*
Here O- Open, H-High, L-Low, C-Close 
CO - Percentage difference between close of a candle(C) to open(O) of a candle (same with  HL)
*/
--Data Cleaning
select * from bn_intra
where --cast(start_time as date) = '2017-10-18'
datepart(hour,start_time) =15 and datepart(minute,start_time) = 30

--Detecting anomalies
with t as(
select cast(start_time as date) 'Date',* from bn_intra
),s as(
select Date,count(*) 'No' from t
group by date  
--order by 'no.',date desc
)
select Date,no from s where no <125 --and no!=20
order by date desc

delete from bn_intra
where cast(start_time as date) in ('2022-03-07') 

--Count of candles in an hour
select datepart(hour,start_time) 'hourwise',
count(*) 'Candle_count_Per_hr'
from bn_intra
where cast(start_time as date) = '2017-11-24'
group by datepart(hour,start_time)
order by datepart(hour,start_time) desc

–Momentum
--Misc Analysis

--Original HL, CO
with t1 as(
select convert(date,start_time) 'Date'
,first_value(O) over(partition by convert(date,start_time) order by start_time) 'O'
,max(H) over(partition by convert(date,start_time) order by start_time) 'H'
,min(L) over(partition by convert(date,start_time) order by start_time) 'L'
,first_value(C) over(partition by convert(date,start_time) order by start_time desc) 'C'
,ROW_NUMBER() over(partition by convert(date,start_time) order by start_time desc) 'rn'
from bn_intra
where convert(time,start_time) !='9:15'
),t2 as(
select Date,datename(dw,date) 'Day'--,lead(c) over(order by date desc) 'PC'
,round((h/l-1)*100,2) 'HL'
,round((c/o-1)*100,2) 'CO'
,round(abs(c/o-1)*100,2) 'AbsCO'
from t1 where rn = 1
)
select * from t2 --where hl<1
order by date desc

--3,9,15,21,30,45,60,90,120,150,180,210 min CO III, IX, XV, XXI, XXX, XLV, LX, XC, CXX, CL, CLXXX,  CCX
with t1 as(
select  *
,case when 
lag(c,2) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,2) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'IX'
,case when 
lag(c,4) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,4) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'XV'
,case when 
lag(c,6) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,6) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'XXI'
,case when 
lag(c,9) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,9) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'XXX'
,case when 
lag(c,14) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,14) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'XLV'
,case when 
lag(c,19) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,19) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'LX'
,case when 
lag(c,29) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,29) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'XC'
,case when 
lag(c,39) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,39) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'CXX'
,case when 
lag(c,49) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,49) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'CL'
,case when 
lag(c,59) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,59) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'CLXXX'
,case when 
lag(c,69) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,69) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end 'CCX'
from bn_intra
--order by start_time desc
)
select start_time,round((C/o-1)*100,2) 'III',round((IX/c-1)*100,2) 'IX',round((XV/c-1)*100,2) 'XV' 
,round((XXI/c-1)*100,2) 'XXI',round((XXX/c-1)*100,2) 'XXX',round((XLV/c-1)*100,2) 'XLV'
,round((LX/c-1)*100,2) 'LX',round((XC/c-1)*100,2) 'XC',round((CXX/c-1)*100,2) 'CXX'
,round((CL/c-1)*100,2) 'CL',round((CLXXX/c-1)*100,2) 'CLXXX',round((CCX/c-1)*100,2) 'CCX'
from t1
order by start_time desc


--To find CO vs duration
with t1 as(
select start_time,C
,max(c) over(partition by convert(date,start_time) order by start_time desc rows between unbounded preceding and current row) 'maxc'
,min(c) over(partition by convert(date,start_time) order by start_time desc rows between unbounded preceding and current row) 'minc'
from bn_intra where convert(time,start_time) !='9:15' and convert(date,start_time) = '2023-4-27'
)
select t1.*,bi.start_time 'maxC_endtime',bi2.start_time 'minC_endtime',round((maxc/t1.c-1)*100,2) 'cmaxc',round((minc/t1.c-1)*100,2) 'cminc'
,datediff(minute,t1.start_time,bi.start_time) 'cmaxc_durn' ,datediff(minute,t1.start_time,bi2.start_time) 'cminc_durn'
from t1 
left join bn_intra bi on convert(date,t1.start_time) =convert(date,bi.start_time) and t1.maxc=bi.c
left join bn_intra bi2 on convert(date,t1.start_time) =convert(date,bi2.start_time) and t1.minc=bi2.c
order by t1.start_time 

--Trending day order by avg_co_intra
select convert(date,start_time)'Date',datename(dw,start_time) 'Day',HL,round(avg(abs(bi.c/bi.o-1))*100,4) 'AvgCO_intra' 
from bn_intra bi left join bn_day bd on convert(date,bi.start_time) = bd.date
where year(start_time) !=2020 --and hl>2 and abs(bi.c/bi.o-1)*100<0.01
group by convert(date,start_time),datename(dw,start_time),hl
order by 'AvgCO_intra' desc

--Day HL, AvgCO_intra order by date
select convert(date,start_time)'Date',datename(dw,start_time) 'Day',HL,round(avg(abs(bi.c/bi.o-1))*100,4) 'AvgCO_intra' 
from bn_intra bi left join bn_day bd on convert(date,bi.start_time) = bd.date
where year(start_time) !=2020 --and hl>2 and abs(bi.c/bi.o-1)*100<0.01
group by convert(date,start_time),datename(dw,start_time),hl
order by 'Date' desc


--Date, Day, HL, Avg_intraCO
with t1 as(
select Date,Day,bd.HL,bi.O,bi.H,bi.L,bi.C from bn_day bd right join bn_intra bi on bd.date = convert(date,bi.start_time) 
)
select Date,Day,HL,round(avg(abs(c/o-1))*100,2) 'Avg_intraCO' from t1 
where hl<1
group by Date,Day,HL
order by date desc 

--Daywise count of candles with co>.2 in intra
with t1 as(
select *,round(abs(c/o-1)*100,2) 'AbsCO' from bn_intra
)
select convert(date,start_time) Date,datename(dw,convert(date,start_time)) 'Day',count(*) X from t1 
where absco>.2 and year(start_time)!=2020 and convert(time,start_time) !='9:15'
group by convert(date,start_time),datename(day,convert(date,start_time))
order by X desc


--HL without 9:15 candle
select convert(date,start_time) 'Date',round((max(H)/min(L)-1)*100,2) as HL from bn_intra
where convert(time,start_time) !='9:15'
group by convert(date,start_time)
order by convert(date,start_time) desc

--60 min
--Day vs hr vs Avg. mmnt
with t1 as(
select *,min(start_time) over(partition by datepart(hour,start_time),convert(date,start_time)) 'start_date'
,first_value(o) over(partition by datepart(hour,start_time),convert(date,start_time) order by start_time) 'Hourly_Open'
,first_value(C) over(partition by datepart(hour,start_time),convert(date,start_time) order by start_time desc) 'Hourly_Close' 
from bn_intra --order by start_time desc
) ,t2 as(
select distinct start_date,Hourly_Open as O, Hourly_Close as C
,round(abs(Hourly_Close/Hourly_Open-1)*100,2) 'CO' from t1 --order by start_date desc
)
select datename(dw,start_date) 'Day',datepart(hour,start_date)'hr',round(avg(co),2) 'Avg_CO' from t2
group by datename(dw,start_date),datepart(hour,start_date)
order by avg(co) desc

--CO vs Time vs X
with t1 as(
select *
,lag(start_time,19) over (order by start_time desc) 'endtime'
,round(abs(lag(C,19) over(order by start_time desc)/O-1)*100,2) 'CO'
from bn_intra
)
select convert(time,start_time) 'Time',CO,count(*) 'X' from t1
where co is not null
group by convert(time,start_time),CO
order by count(*) desc

--Hourwise Avg_AbsCO 
with t1 as(
select start_time,O,C,lag(C,19) over(order by start_time desc) 'candle_C' from bn_intra
),t2 as(
select *,round(((candle_C/O)-1)*100,2) 'CO' from t1
where convert(time,start_time) <='14:30' and convert(time,start_time) !='9:15' 
)
select datepart(hour,start_time) 'start_time', count(*) 'times',round(avg(abs(co)),2) 'avg_co' from t2
group by datepart(hour,start_time)
order by avg(abs(co)) desc,start_time desc

--Count AbsCO>.5
with t1 as(
select start_time,O,C,lag(C,19) over(order by start_time desc) 'candle_C' 
from bn_intra
),t2 as(
select *,round(((candle_C/O)-1)*100,2) 'CO' from t1
where convert(time,start_time) <='14:27' and convert(time,start_time) !='9:15'
)
select convert(time,start_time) 'start_time', count(*) 'times' from t2
where abs(co)>.5 --and datename(dw,start_time) = 'Thursday'
group by convert(time,start_time)
order by 'times' desc

--CO
with t1 as(
select  *,case when 
lag(c,19) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,19) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end prev
from bn_intra
--order by start_time desc
)
select *,round((prev/c-1)*100,2) 'CO' from t1
order by start_time desc

– Avg_AbsCO
with t1 as(
select *
,lag(start_time,19) over (order by start_time desc) 'endtime'
,round(abs(lag(C,19) over(order by start_time desc)/O-1)*100,2) 'AbsCO'
from bn_intra where convert(time,start_time) != '9:15' --datename(weekday,cast(start_time as date)) = 'Thursday'
)
select cast(start_time as time) 'starttime',round(avg(AbsCO),2) 'Avg_AbsCO' from t1
where cast(start_time as date) = cast(endtime as date) 
group by cast(start_time as time) 
order by avg(AbsCO) desc

--Avg_AbsCO absco>.3
with t as(
select start_time,O,C,lag(C,19) over(order by start_time desc) 'candle_C' 
from bn_intra
),r as(
select *,round(((candle_C/O)-1)*100,2) 'CO' from t 
where convert(time,start_time) <='14:30' and convert(time,start_time) !='9:15'
)
select convert(time,start_time) 'start_time', count(*) 'times',round(avg(abs(co)),2) 'avg_co' from r
group by convert(time,start_time)
order by avg(abs(co)) desc



--Avg_AbsCO absco>.4
with t1 as(
select *,min(start_time) over(partition by datepart(hour,start_time),convert(date,start_time)) 'start_date'
,first_value(o) over(partition by datepart(hour,start_time),convert(date,start_time) order by start_time) 'Hourly_Open'
,first_value(C) over(partition by datepart(hour,start_time),convert(date,start_time) order by start_time desc) 'Hourly_Close' 
from bn_intra --order by start_time desc
),t2 as(
select distinct start_date,Hourly_Open as O, Hourly_Close as C
,round(abs(Hourly_Close/Hourly_Open-1)*100,2) 'CO' from t1 --order by start_date desc
)
select datepart(hour,start_date)'hr',round(avg(co),2) 'Avg_CO' from t2 where abs(co)>.4
group by datepart(hour,start_date)
order by avg(co) desc

--Order by max mmnts 
with t1 as(
select start_time,O,C,lag(C,19) over(order by start_time desc) 'candle_C' from bn_intra
),t2 as(
select *,round(((candle_C/O)-1)*100,2) 'CO' from t1
where convert(time,start_time) <='14:30' and convert(time,start_time) !='9:15'
)
select convert(date,start_time) as Date,max(abs(co)) as CO from t2
where abs(co)>.5 and year(start_time)!=2020 --and datename(dw,start_time) = 'Thursday'
group by convert(date,start_time)
order by CO desc

--21 min
--Avg_AbsCO, X when co>.3
with t1 as(
select *
,lag(start_time,6) over (order by start_time desc) 'endtime'
,round(abs(lag(C,6) over(order by start_time desc)/O-1)*100,2) 'CO'
from bn_intra where convert(time,start_time) != '9:15' 
)
select cast(start_time as time) 'starttime',round(avg(co),2) 'Avg_CO',count(*) 'X' from t1
where cast(start_time as date) = cast(endtime as date) and co>.3
group by cast(start_time as time) 
order by avg(co) desc, 'X' desc

--Order by CO>.3 
with t as(
select  *
, lag([start_time],6) OVER (Order by start_time desc) as 'endtime',
round(abs(lag(C,6) OVER (Order by start_time desc)/O-1)*100,2) as 'percent' 
from bn_intra where cast(start_time as time)!='9:15' 
),r as(
select *,case when start_time>DATEADD( minute,18,lead([start_time],1) OVER (Order by start_time desc)) then 1 else 0 end as 'col' from t 
where 
[percent]>=0.3 and 
cast(start_time as date) = cast(endtime as date) 
)
select [start_time], [percent] from r where col =1

–+ve and -ve trends 

--15 min
--Hourwise Avg_AbsCO X (9,1,2,3 hrs)
with t1 as(
select *
,lag(start_time,4) over (order by start_time desc) 'endtime'
,round(abs(lag(C,4) over(order by start_time desc)/O-1)*100,2) 'AbsCO'
from bn_intra
)
select datepart(hour,start_time) 'Hour', round(avg(absco),2) 'Avg_AbsCO', count(*) 'X' 
from t1 where absco>.3
group by datepart(hour,start_time)
order by 'Avg_AbsCO' desc

--CO
with t1 as(
select  *,case when 
lag(c,4) over(partition by convert(date,start_time) order by start_time desc) is not null then 
lag(c,4) over(partition by convert(date,start_time) order by start_time desc) else
FIRST_VALUE(C) over(partition by convert(date,start_time) order by start_time desc) end prev
from bn_intra
--order by start_time desc
)
select *,round((prev/c-1)*100,2) 'CO' from t1
order by start_time desc

--CO vs trend
with t as(
select start_time,O,C,lag(C,4) over(order by start_time desc) 'candle_C' 
from bn_intra 
),r as(
select *,round(((candle_C/O)-1)*100,2) 'CO' from t
where convert(time,start_time) <='15:15' and convert(time,start_time) !='9:15'
)
select *, case when co>0 then 'P' else 'N' end as Trend from r 
order by start_time desc


--Time vs Avg_AbsCO vs X when AbsCO>.3
with t as(
select start_time,O,C,lag(C,4) over(order by start_time desc) 'candle_C' 
from bn_intra
),r as(
select *,round(((candle_C/O)-1)*100,2) 'CO' from t 
where convert(time,start_time) <='15:15' and convert(time,start_time) !='9:15'
)
select convert(time,start_time) 'start_time',round(avg(abs(co)),2) 'avg_co', count(*) 'X' from r
where abs(co)>.3
group by convert(time,start_time)
order by count(*) desc
