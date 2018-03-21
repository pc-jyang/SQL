/*
* missing pay-to-name
*/
select
  commission_batch_id as " Batch Id"
, redshift_id as "Commission Id"
, x.dw_encrypted_name as "Client Name"
, status as "Status"
, user_id as "User Id"
, emp_role as "Employee Role"
, comm_type as "Commission Type"
, to_char(comm_date,'MM/DD/YYYY') as "Commission Run Date"
, to_char(initial_trading_date, 'MM/DD/YYYY') as "Initial Trading Date"
, pay_to_name as "Pay To Name"
, pay_to_id as "Pay To Id"
, base_amt as "Base Amount"
, comm_total_amt as "Commission Total Amount"
, is_terminated as "Terminated"
, period as "Period"
, user_guid as "User Guid"
, account_id as "SF Account Id"
, lead_id as "SF Lead Id"
, task_id as "SF Task Id"
, to_char(comm_term_date, 'MM/DD/YYYY') as "Commission Term Date"
, to_char(term_date, 'MM/DD/YYYY') as "Termination Date"
, to_char(redshift_created_date, 'MM/DD/YYYY HH24:MI:SS') as "Created Date"
, to_char(redshift_updated_date, 'MM/DD/YYYY HH24:MI:SS') as "Updated Date"
from jyang_workspace.commission x
where pay_to_name is null 
and x.commission_batch_id = f_get_commission_batch_id();

/*
pay-to-not -equal-owner :: incentive-program-sf-payto-not-equal-current
*/
select
  commission_batch_id as " Batch Id"
, redshift_id as "Commission Id"
, x.dw_encrypted_name as "Client Name"
, status as "Status"
, user_id as "User Id"
, emp_role as "Employee Role"
, comm_type as "Commission Type"
, to_char(comm_date,'MM/DD/YYYY') as "Commission Run Date"
, to_char(initial_trading_date, 'MM/DD/YYYY') as "Initial Trading Date"
, x.financial_advisor_name as "Current Financial Advisor"
, x.financial_advisor_id
, pay_to_name as "Pay To Name"
, pay_to_id as "Pay To Id"
, base_amt as "Base Amount"
, comm_total_amt as "Commission Total Amount"
, is_terminated as "Terminated"
, period as "Period"
, user_guid as "User Guid"
, account_id as "SF Account Id"
, to_char(comm_term_date, 'MM/DD/YYYY') as "Commission Term Date"
, to_char(term_date, 'MM/DD/YYYY') as "Termination Date"
, to_char(redshift_created_date, 'MM/DD/YYYY HH24:MI:SS') as "Created Date"
, to_char(redshift_updated_date, 'MM/DD/YYYY HH24:MI:SS') as "Updated Date"
from jyang_workspace.commission x
where program_name = 'Financial Advisor - Fundings'
and nvl(financial_advisor_name,'x') <> nvl(pay_to_name, 'y') 
and nvl(financial_advisor_name || ' CFP','x') <> nvl(pay_to_name, 'y')
and x.commission_batch_id = f_get_commission_batch_id() 
and x.pay_to_name is not null
and financial_advisor_name != 'Dan Stampf';

/*
* incentive-program-sf-trading-not-trading-oltp
*/
select u.user_id
     , a.user_guid
     , u.encrypted_myvest_household_id
     , a.investment_closed_trading_date
     , a.investment_closed_term_date
     , a.created_date salesforcde_create_date, u.is_data_source_oracle_dw, u.is_deleted, is_suspicious
from  salesforce_schema.account a
join sp_schema.user u on a.user_id = u.user_id
left outer join sp_schema.aum_initial_trading_by_user aum on a.user_id = aum.user_id
where 
-- u.is_myvest_dup_or_test = 0
-- and  
a.investment_closed_trading_date is not null
and aum.user_id is null
and u.encrypted_myvest_household_id is null
and u.user_id not in ( 22815, 148770, 232792 );
