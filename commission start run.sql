select CONVERT(BIGINT, TO_CHAR(getdate(),'YYYYMMDDHH24MI'));

insert into jyang_workspace.commission_batch(commission_batch_id, commission_start_date, redshift_created_date, redshift_updated_date) 
values (201803152038, TO_DATE('2018-03-01','YYYY-MM-DD'), sysdate, sysdate);

select * from jyang_workspace.commission_batch order by commission_batch_id desc;

CREATE OR REPLACE FUNCTION f_get_commission_batch_id ()
  returns BIGINT
stable
as $$
  SELECT 201803152038
$$ language sql;

 /* * * *
  * SET commission month
  * * * * */
CREATE OR REPLACE FUNCTION f_get_commission_month ()
  returns DATE
stable
as $$
  SELECT TO_DATE('2018-03-01','YYYY-MM-DD')
$$ language sql;

select f_get_commission_batch_id();

select f_get_commission_month();