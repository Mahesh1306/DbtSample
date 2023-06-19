with muid as 
(Select ID, 'dummy' as New_Last_Name,
RANK() OVER(
ORDER BY last_name
) Rank_no  from raw.jaffle_shop.employees where Rank_no != 2 group by first_name)

Select * from muid

Update employees set last_name = muid.New_Last_Name where ID = muid.Rank_no


