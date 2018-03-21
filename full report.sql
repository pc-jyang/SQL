/*
fa-fundings-detail
*/
select
    pay_to_name as "Pay To"
  , 'Client' as "Line Type"
  , pcg_financial_advisor_name as "PCG Financial Advisor"
  , dw_encrypted_name as "Client Name"
  , user_guid as "User Guid"
  , redshift_id as "Commission Reference Id"
  , to_char(comm_date, 'MM/DD/YYYY')  as "Commission Run"
  , comm_type  as "Commission Type"
  , period as "Period"
  , to_char(initial_trading_date, 'MM/DD/YYYY') "Initial Trading Date"
  , 1 as "Units"
  , t.base_amt as "AUM Balance or Additions"
  , comm_total_amt as "Total Commission Amount"
  , notes as "Notes"
from jyang_workspace.commission t
where
t.commission_batch_id = f_get_commission_batch_id ()
and program_name ='Financial Advisor - Fundings'
union all
select
    pay_to_name as "Pay To"
  , 'Total'
  , null
  , null
  , null
  , null
  , null
  , null
  , null
  , null
  , count(1)
  , sum(t.base_amt) as "AUM Balance/Additions"
  , sum(comm_total_amt) as "Total Commission Amount"
  , null
from jyang_workspace.commission t
where
t.commission_batch_id = f_get_commission_batch_id ()
and program_name ='Financial Advisor - Fundings'
group by pay_to_name
order by 1, 5, 2 desc, 4;

/*
fa-fundings-sum
*/
select
    pay_to_name as "Pay To"
  , count(1) as "Units"
  , sum(comm_total_amt) as "Total Commission Amount"
from jyang_workspace.commission t
where
t.commission_batch_id = f_get_commission_batch_id ()
and program_name ='Financial Advisor - Fundings'
group by pay_to_name
union all
select 'z Total' as "Pay To"
		, count(1) as "Units"
		, sum (comm_total_amt)as "Total Commission Amount"
from jyang_workspace.commission
where commission_batch_id = f_get_commission_batch_id ()
	and program_name ='Financial Advisor - Fundings'
order by "Pay To";

/*
sa-fundings-detail
*/
select
    pay_to_name as "Pay To"
  , 'Client' as "Line Type"
  , dw_encrypted_name as "Client Name"
  , user_guid as "User Guid"
  , redshift_id as "Commission Reference Id"
  , to_char(comm_date, 'MM/DD/YYYY')  as "Commission Run"
  , comm_type  as "Commission Type"
  , period as "Period"
  , to_char(initial_trading_date, 'MM/DD/YYYY') "Initial Trading Date"
  , 1 as "Units"
  , t.base_amt as "Additions"
  , comm_total_amt as "Total Commission Amount"
from jyang_workspace.commission t
where
t.commission_batch_id = f_get_commission_batch_id ()
and comm_type = 'initial-funding'
and program_name ='Sales Associate - Fundings'
union all
select
    pay_to_name as "Pay To"
  , 'Total'
  , null
  , null
  , null
  , null
  , null
  , null
  , null
  , count(1)
  , sum(t.base_amt) as "Additions"
  , sum(comm_total_amt) as "Total Commission Amount"
from jyang_workspace.commission t
where
t.commission_batch_id = f_get_commission_batch_id ()
and comm_type = 'initial-funding'
and program_name ='Sales Associate - Fundings'
group by pay_to_name
order by 1, 5, 2 desc, 4;

/*
sa-fundings-sum
*/
select
    pay_to_name as "Pay To"
  , count(1) as "Units"
  , sum(comm_total_amt) as "Total Commission Amount"
from jyang_workspace.commission t
where
t.commission_batch_id = f_get_commission_batch_id ()
and comm_type = 'initial-funding'
and program_name ='Sales Associate - Fundings'
group by pay_to_name
union all
select 'z Total' as "Pay To"
		, count(1) as "Units"
		, sum (comm_total_amt)as "Total Commission Amount"
from jyang_workspace.commission
where commission_batch_id = f_get_commission_batch_id ()
	and comm_type = 'initial-funding'
and program_name ='Sales Associate - Fundings'
order by "Pay To";

/*
sa-meeting-set-detail
*/
select 
    pay_to_name as "Pay To"
  , 'Client' as "Line Type"
  , dw_encrypted_name as "Client Name"
  , user_guid as "User Guid"
  , redshift_id as "Commission Reference Id"
  , to_char(comm_date, 'MM/DD/YYYY')  as "Commission Run Date"
  , comm_type  as "Commission Type"
  , period as "Period"
  , sa_meet_set_created_by_name "Meeting Set by"
  , to_char(sa_meet_set_created_date, 'MM/DD/YYYY MI:HH') "Meeting Set Date"
  , fa_meet_held_created_by_name "Meeting Held By"
  , to_char(t.fa_meet_held_created_date, 'MM/DD/YYYY MI:HH') "Meeting Held Date"
  , fa_meet_held_type as "Meeting Held Type"
  , 1 as "Units"
 , comm_total_amt as "Total Commission Amount" 
from jyang_workspace.commission t
where
t.commission_batch_id = f_get_commission_batch_id ()
and comm_type = 'sa-meeting-set-fa-ig-held'
union all
select
    pay_to_name as "Pay To"
  , 'Total'
  , null
  , null
  , null
  , null
  , null
  , null
  , null
  , null
  , null
  , null
  , null
  , count(1)
  , sum(comm_total_amt) as "Total Commission Amount" 
from jyang_workspace.commission t
where 
t.commission_batch_id = f_get_commission_batch_id ()
and comm_type = 'sa-meeting-set-fa-ig-held'
group by pay_to_name
order by 1, 2, 4, 5;

/*
sa-meeting-set-sum
*/
select
    pay_to_name as "Pay To"
  , count(1) as "Units"
  , sum(comm_total_amt) as "Total Commission Amount"
from jyang_workspace.commission t
where
t.commission_batch_id = f_get_commission_batch_id ()
and comm_type = 'sa-meeting-set-fa-ig-held'
group by pay_to_name
union all
select 'z Total' as "Pay To"
		, count(1) as "Units"
		, sum (comm_total_amt)as "Total Commission Amount"
from jyang_workspace.commission
where commission_batch_id = f_get_commission_batch_id ()
and comm_type = 'sa-meeting-set-fa-ig-held'
order by "Pay To";

/*
* full detail
*/
select
  commission_batch_id
, redshift_id
, status
, user_id
, emp_role
, comm_type
, to_char(comm_date,'MM/DD/YYYY') comm_date
, pay_to_name
, pcg_financial_advisor_id
, pcg_financial_advisor_name
, base_amt
, comm_total_amt
, base_rate
, bonus_rate
, total_rate
, comm_base_rate_amt
, comm_bonus_rate_amt
, is_terminated
, period
, pay_to_id
, user_guid
, account_id
, lead_id
, task_id
, to_char(comm_term_date, 'MM/DD/YYYY') comm_term_date
, to_char(term_date, 'MM/DD/YYYY') term_date
, to_char(term_month_end_date, 'MM/DD/YYYY') term_month_end_date
, to_char(initial_trading_date, 'MM/DD/YYYY') initial_trading_date
, to_char(initial_trading_month_end_date, 'MM/DD/YYYY') initial_trading_month_end_date
, initial_trading_amt
, additional_amt
, first_financial_advisor_name
, financial_advisor_name
, opportunity_owner_name
, opportunity_owner_id
, to_char(sa_meet_set_created_date, 'MM/DD/YYYY') sa_meeting_set_created_date
, sa_meet_set_created_by_id
, sa_meet_set_created_by_name
, to_char(fa_meet_held_created_date, 'MM/DD/YYYY') fa_held_created_date
, fa_meet_held_created_by_id
, fa_meet_held_created_by_name
, fa_meet_held_type
, to_char(redshift_created_date, 'MM/DD/YYYY HH24:MI:SS') created_date
, to_char(redshift_updated_date, 'MM/DD/YYYY HH24:MI:SS') updated_date
from jyang_workspace.commission x
where x.commission_batch_id =  f_get_commission_batch_id ();

/*
* full summery by "Pay To"
*/
select
  pay_to_name as "Pay To"
, program_name as "Program/Totals"
, count(1) as "Unit Count"
, sum(comm_total_amt) as "Commission Total"
from jyang_workspace.commission x
where x.commission_batch_id = f_get_commission_batch_id ()
group by pay_to_name, program_name
union all
select
  pay_to_name
, 'Total'
, count(1)
, sum(comm_total_amt)
from jyang_workspace.commission x
where x.commission_batch_id = f_get_commission_batch_id ()
group by pay_to_name
order by 1, 2;

/*
* program-summary :: incentive-program-summary
*/
select t.program_name, comm_type, period, sum(comm_total_amt) comm_total_amt, count(1) cnt
 from jyang_workspace.commission t 
where t.commission_batch_id = f_get_commission_batch_id ()
group by t.program_name, comm_type, period
order by 1, 2, 3 desc; 

