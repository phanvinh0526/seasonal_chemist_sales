/*
 * 		EDA
 * */

-- Calculate median value
drop procedure if exists proc_median;

create procedure proc_median(in tbl_name varchar(50), in col_name varchar(50))
begin
	declare median_val float default 0.0;

	set @sql = replace('
			SELECT AVG(dd.col_name) into median_val
			FROM (
			SELECT d.col_name, @rownum:=@rownum+1 as `row_number`, @total_rows:=@rownum
			  FROM vw_sales_n_weather d, (SELECT @rownum:=0) r
			  WHERE d.col_name is NOT NULL
			  -- put some where clause here
			  ORDER BY d.col_name
			) as dd
			WHERE dd.row_number IN ( FLOOR((@total_rows+1)/2), FLOOR((@total_rows+2)/2) )
	', 'col_name', col_name);
	prepare stmt from @sql;
	execute stmt;
	deallocate prepare stmt;
	
-- 	return median_val;
end;


create function proc_median_temp() returns float
begin
	declare median_val float;
	SELECT AVG(dd.temp) into median_val
	FROM (
	SELECT d.temp, @rownum:=@rownum+1 as `row_number`, @total_rows:=@rownum
	  FROM vw_sales_n_weather d, (SELECT @rownum:=0) r
	  WHERE d.temp is NOT NULL
	  -- put some where clause here
	  ORDER BY d.temp
	) as dd
	WHERE dd.row_number IN ( FLOOR((@total_rows+1)/2), FLOOR((@total_rows+2)/2) );
	return round(median_val,2);
end;

create function proc_median_humid() returns float
begin
	declare median_val float;
	SELECT AVG(dd.humidity) into median_val
	FROM (
	SELECT d.humidity, @rownum:=@rownum+1 as `row_number`, @total_rows:=@rownum
	  FROM vw_sales_n_weather d, (SELECT @rownum:=0) r
	  WHERE d.humidity is NOT NULL
	  -- put some where clause here
	  ORDER BY d.humidity
	) as dd
	WHERE dd.row_number IN ( FLOOR((@total_rows+1)/2), FLOOR((@total_rows+2)/2) );
	return round(median_val,2);
end;

create function proc_median_wind() returns float
begin
	declare median_val float;
	SELECT AVG(dd.windspeed) into median_val
	FROM (
	SELECT d.windspeed, @rownum:=@rownum+1 as `row_number`, @total_rows:=@rownum
	  FROM vw_sales_n_weather d, (SELECT @rownum:=0) r
	  WHERE d.windspeed is NOT NULL
	  -- put some where clause here
	  ORDER BY d.windspeed
	) as dd
	WHERE dd.row_number IN ( FLOOR((@total_rows+1)/2), FLOOR((@total_rows+2)/2) );
	return round(median_val,2);
end;

/*
 * 		
 * */

-- Number of product sold per category
select product_cat, count(*) from vw_sales_n_weather group by product_cat 
Cold and Flu	707
Hayfever		730
Cosmetics		363
Hair Care		365

-- Number of sales sold per category
select product_cat, sum(qty_sold) from vw_sales_n_weather group by product_cat 
Hayfever		29024
Cold and Flu	10265
Cosmetics		10085
Hair Care		6197

-- Number of sales per weather condition
select conditions, sum(qty_sold) from vw_sales_n_weather vsnw group by conditions 
Rain, Partially cloudy	25442
Partially cloudy		16514
Rain, Overcast			7479
Overcast				1803
Clear					2553
Rain					1780

-- Check outlier
select max(temp), min(temp), proc_median_temp() from vw_sales_n_weather vsnw ;
27.6	7.4		15.4
select max(humidity), min(humidity), proc_median_humid() from vw_sales_n_weather vsnw ;
97.8	30.1	69.2
select max(windspeed), min(windspeed), proc_median_wind() from vw_sales_n_weather vsnw ;
33.8	7.1		17.1



/*
 * 		Hypothesis
 * */
-- What are they buying on rainy days
select
	'On Rainy Days' 
	,v.product_cat 
	,sum(v.qty_sold)
from vw_sales_n_weather v 
where
	v.conditions  like '%Rain%'
group by
	v.product_cat 
order by sum(v.qty_sold)
	
On Rainy Days	Hair Care	3894
On Rainy Days	Cold and Flu	6793
On Rainy Days	Cosmetics	6531
On Rainy Days	Hayfever	17483

=>	Ppl buy more hayfever items on rainy day -- ***


-- When customers buying more Cosmetics
SELECT 
	'Cosmetics'
	,year(v.dt) yr, month(v.dt) mth
	,sum(v.qty_sold)
from vw_sales_n_weather v
where v.product_cat = 'Cosmetics'
group by 
	year(v.dt), month(v.dt)
order by year(v.dt), month(v.dt) desc

=> There are more sales on Cosmetics between Sep and Dec
=> Normally, Cosmetics & perfume have higher margin than other products in a pharmacy


-- When customers buying more Cosmetics
SELECT 
	'Cold and Flu'
	,year(v.dt) yr, month(v.dt) mth
	,sum(v.qty_sold)
from vw_sales_n_weather v
where v.product_cat = 'Cold and Flu'
group by 
	year(v.dt), month(v.dt)
order by year(v.dt), month(v.dt) desc

=> Ppl are more likely to buy Cold & flu between May and July as the winder coming
=> Flu vaccination is also at high demand during this period of a year





