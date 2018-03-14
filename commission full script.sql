 /*select CONVERT(BIGINT, TO_CHAR(getdate(),'YYYYMMDDHH24MI'));

insert into jyang_workspace.commission_batch(commission_batch_id, commission_start_date, redshift_created_date, redshift_updated_date) 
values (201803072144, TO_DATE('2018-03-01','YYYY-MM-DD'), sysdate, sysdate);

select * from jyang_workspace.commission_batch order by commission_batch_id desc;


-- 201803052111
select max(commission_batch_id) from jyang_workspace.commission_batch;

delete jyang_workspace.COMMISSION;

delete jyang_workspace.COMMISSION_ADD_FUNDS_DETAIL;

delete  jyang_workspace.COMMISSION_aum_detail;
*/
 /* * * *
  * SET commission batch id
  * * * * */
CREATE OR REPLACE FUNCTION f_get_commission_batch_id ()
  returns BIGINT
stable
as $$
  SELECT 201803072144
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

 /* * * *
  *
  * Add initial trading for fa role
  *
  * * * * */
PREPARE prep_load_FA_InitialTrading (bigint, DATE) AS 
insert into jyang_workspace.COMMISSION
        (commission_batch_id
        , program_name
          , account_id
          , dw_encrypted_name
          , user_id
          , is_terminated
          , comm_date
          , comm_term_date
          , term_date
          , term_month_end_date
          , comm_type
          , period
          , emp_role
          , initial_trading_date
          , initial_trading_month_end_date
          , initial_trading_amt
          , base_rate
          , bonus_rate
          , total_rate
          , redshift_created_date
          , redshift_updated_date
        )
 select $1, program_name
          , account_id, null
          , user_id
          , is_terminated
          , comm_date
          , comm_term_date
          , term_date
          , last_day(term_date) term_month_end_date
          , comm_type
          , period
          , emp_role
          , initial_trading_date
          , initial_trading_month_end_date
          , initial_trading_amt
          , base_rate
          , bonus_rate
          , total_rate
          , sysdate
          , sysdate
        from (
                select a.account_id
                    , xc.program_name
                    , au.user_id
                    , decode(a.investment_closed_term_date, null, 0, 1) is_terminated
                    , add_months(last_day(initial_trading_date) + 1, xc.add_months) comm_date
                    , case when a.investment_closed_term_date is null then add_months(last_day(initial_trading_date) + 1, 12)
                           when last_day(trunc(a.investment_closed_term_date))+ 1 < add_months(last_day(initial_trading_date) + 1, 12) 
                                then last_day(trunc(a.investment_closed_term_date)) + 1
                           else add_months(last_day(initial_trading_date) + 1, 12)
                      end comm_term_date
                    , trunc(a.investment_closed_term_date) term_date
                    , xc.comm_type
                    , xc.period
                    , xc.emp_role
                    , initial_trading_date
                    , last_day(initial_trading_date) initial_trading_month_end_date
                    , au.initial_trading_amt
                    , xc.base_rate                                  
                    , xc.bonus_rate
                    , base_rate +  xc.bonus_rate total_rate
                from   sp_schema.aum_initial_trading_by_user au
                join salesforce_schema.account a on au.user_id = a.user_id
                cross join jyang_workspace.commission_config xc
                where  xc.comm_type = 'initial-funding' and xc.emp_role = 'fa'
                and last_day(initial_trading_date) between xc.initial_funding_start_date and xc.initial_funding_end_date
                and add_months(last_day(initial_trading_date) + 1, xc.add_months) >= $2
            ) com
        where com.comm_date <= com.comm_term_date
        and   com.comm_date < nvl(com.term_date,sysdate+1)  -- account for term date
        and com.comm_date <= trunc(sysdate);

 /* * * *
  *
  * Add cash additions for fa role
  *
  * * * * */  
PREPARE prep_load_FA_CashAdditions (bigint, DATE) AS
insert into jyang_workspace.commission
        ( commission_batch_id
          , program_name
          , account_id
          , dw_encrypted_name
          , user_id
          , is_terminated
          , comm_date
          , comm_term_date
          , term_date
          , term_month_end_date
          , comm_type
          , period
          , emp_role
          , initial_trading_date
          , initial_trading_month_end_date
          , additional_amt
          , additional_last_day_tran_date
          , base_amt
          , base_rate
          , bonus_rate
          , total_rate
          , redshift_created_date
          , redshift_updated_date
        )
select  $1, program_name
          , account_id
          , null -- dw_encrypted_name
          , user_id
          , is_terminated
          , comm_date
          , comm_term_date
          , term_date
          , last_day(term_date) term_month_end_date
          , comm_type
          , period
          , emp_role
          , initial_trading_date
          , last_day(initial_trading_date) initial_trading_month_end_date
          , additional_cash_in
          , additional_last_day_tran_date
          , additional_cash_in
          , base_rate
          , bonus_rate
          , total_rate
          , sysdate
          , sysdate
        from (
                select  max(xc.program_name) program_name
                    , a.account_id
                    -- , max(a.dw_encrypted_name) dw_encrypted_name
                    , v.user_id
                    , xc.comm_type
                    , xc.period
                    , add_months(tran_date_first, xc.add_months)         comm_date
                    , sum(additional_cash_in)                            additional_cash_in
                    , max(additional_last_day_tran_date)                 additional_last_day_tran_date
                    , max(initial_trading_date)                          initial_trading_date
                    , max(xc.base_rate)                                  base_rate
                    , 0                                                  bonus_rate
                    , 0                                                  bonus_rate_2013
                    , max(xc.base_rate)                                  total_rate
                    , sum(additional_cash_in)                            raw_balance
                    , max(case when a.investment_closed_term_date is null then add_months(last_day(initial_trading_date) + 1, max_term_months)
                               when last_day(trunc(a.investment_closed_term_date)) + 1 < add_months(last_day(initial_trading_date) + 1, xc.max_term_months) 
                                    then last_day(trunc(a.investment_closed_term_date)) + 1
                               else add_months(last_day(initial_trading_date) + 1, max_term_months)
                          end) comm_term_date
                    , max(decode(a.investment_closed_term_date, null, 0, 1)) is_terminated
                    , max(trunc(a.investment_closed_term_date)) term_date
                    , max(emp_role)                           emp_role
                from (select user_id
                            , tran_date_first
                            , initial_trading_date
                            , additional_cash_in
                            , additional_last_day_tran_date
                            , comm_type
                      from (
                              select 'new' method, user_id
                                , last_day(tran_date) + 1 tran_date_first
                                , max(initial_trading_date) initial_trading_date
                                , sum(t.additional_cash_in) additional_cash_in
                                , max(last_day(tran_date))  additional_last_day_tran_date
                                , case when months_between(last_day(tran_date), last_day(initial_trading_date)) <= 12 
                                        then 'additions-year-1' else 'additions-year-2-3' end comm_type
                              from   sp_schema.aum_tran_date_account_funding t
                              where 
                                -- changed to 37 just to make sure we get the last months (should be okay since they would not qualify past this)
                                months_between(tran_date, initial_trading_date) < 37
                              and  tran_date >= initial_trading_date 
                              -- add_months(startDate,-2) -- subtract month out since we run on commissions date
                              group by user_id, last_day(tran_date) + 1, 
                              case when months_between(last_day(tran_date), last_day(initial_trading_date)) <= 12 
                              then 'additions-year-1' else 'additions-year-2-3' end 
                            ) where additional_cash_in >= 25000
                     ) v
                left outer join salesforce_schema.account a on v.user_id = a.user_id
                cross join jyang_workspace.commission_config xc
                where  xc.comm_type = v.comm_type and xc.emp_role = 'fa'
                and last_day(initial_trading_date) between xc.initial_funding_start_date and xc.initial_funding_end_date
                and add_months(tran_date_first, xc.add_months) < trunc(sysdate)  
                and add_months(tran_date_first, xc.add_months) >= add_months('2010-01-01',-2)
                group  by xc.comm_type
                            ,xc.period
                            ,a.account_id
                            ,v.user_id
                            ,add_months(tran_date_first, xc.add_months) -- comm_date
        ) com
        where com.comm_date <= com.comm_term_date
        and   com.comm_date < nvl(com.term_date,sysdate+1)  -- account for term date
        and com.comm_date <= trunc(sysdate)
        and com.comm_date >= $2  and com.comm_date <= trunc(sysdate)
        and com.comm_date <= $2
        order by user_id, comm_type, period;
   
/* store audit data for reference */     
PREPARE prep_store_audit_data (bigint) AS
insert into jyang_workspace.commission_add_funds_detail
        (     
            commission_batch_id 
            , comm_type
            , comm_date
            , user_id
            , user_account_id
            , tran_date
            , additional_cash_in
            , redshift_created_date
        )
        select $1
            , c.comm_type
            , c.comm_date
            , c.user_id
            , t.user_account_id
            , t.tran_date
            , t.additional_cash_in
            , sysdate
        from jyang_workspace.commission c
        join sp_schema.aum_tran_date_account_funding t 
        on c.user_id = t.user_id and t.tran_date > add_months(c.additional_last_day_tran_date,-1) and t.tran_date <= c.additional_last_day_tran_date
        where 
            commission_batch_id = $1 and
            comm_type like '%addition%' and
            t.additional_cash_in > 0;

 /* * * *
  *
  * Add sales associate initial trading for sa role
  *
  * * * * */
PREPARE prep_load_SA_InitialTrading (bigint, DATE) AS
insert into jyang_workspace.commission
        ( commission_batch_id
          , program_name
          , account_id
          , dw_encrypted_name
          , user_id
          , user_guid
          , first_financial_advisor_name
          , financial_advisor_name
          , is_terminated
          , comm_date
          , comm_term_date
          , term_date
          , term_month_end_date
          , comm_type
          , period
          , emp_role
          , pay_to_id
          , pay_to_name
          , sa_meet_set_created_date
          , sa_meet_set_created_by_id
          , sa_meet_set_created_by_name
          , initial_trading_date
          , initial_trading_month_end_date
          , initial_trading_amt
          , base_rate
          , bonus_rate
          , total_rate
          , redshift_created_date
          , redshift_updated_date
        )
        select $1
           , program_name
           , v.account_id
           , null
           , v.user_id
           , v.user_guid
           , first_financial_advisor_name
           , financial_advisor_name
           , is_terminated
           , comm_date
           , comm_term_date
           , term_date
           , last_day(term_date) term_month_end_date
           , comm_type
           , period
           , emp_role
           , pay_to_id
           , pay_to_name
           , sa_meeting_set_created_date
           , sa_meeting_set_created_by_id
           , sa_meeting_set_created_by_name
           , initial_trading_date
           , initial_trading_month_end_date
           , initial_trading_amt
           , base_rate
           , bonus_rate
           , total_rate
           , sysdate
           , sysdate
        from 
        (select 
            program_name
          , a.account_id
          -- , a.dw_encrypted_name
          , aum.user_id
          , a.user_guid
          , a.first_fin_advisor_assign_name first_financial_advisor_name
          , a.owner_id financial_advisor_id
          , a.owner_name financial_advisor_name
          , decode(a.investment_closed_term_date, null, 0, 1) is_terminated
          , add_months(last_day(initial_trading_date) + 1, xc.add_months) comm_date
          , add_months(last_day(initial_trading_date) + 1, xc.add_months) comm_term_date
          , trunc(a.investment_closed_term_date) term_date
          , xc.comm_type
          , xc.period
          , xc.emp_role
          , initial_trading_date
          , last_day(initial_trading_date) initial_trading_month_end_date
          , aum.initial_trading_amt
          , xc.base_rate                                  
          , xc.bonus_rate
          , base_rate + xc.bonus_rate total_rate
          , t.owner_id pay_to_id
          , t.owner_name pay_to_name
          , t.created_date sa_meeting_set_created_date
          , t.owner_id sa_meeting_set_created_by_id
          , t.owner_name sa_meeting_set_created_by_name
          , row_number() over (partition by aum.user_id order by aum.initial_trading_date - t.created_date nulls last) sort_seq
        from sp_schema.aum_initial_trading_by_user aum
        join salesforce_schema.account a on aum.user_id = a.user_id
        join salesforce_schema.task t on a.account_id = t.account_id and t.created_by_name <> 'api user'
        cross join jyang_workspace.commission_config xc
        where t.created_date < aum.initial_trading_date
        and xc.comm_type = 'initial-funding' and xc.emp_role = 'sa'
        and t.type = 'SA Meeting Set'
        and add_months(last_day(initial_trading_date) + 1, xc.add_months) >= $2
        ) v
        where sort_seq = 1
        and comm_date <= trunc(sysdate);
        
 /* * * *
  *
  * Load SA Meetings Set With IG 
  *
  * * * * */
PREPARE prep_load_SA_MeetingsSetWithIG (bigint, DATE) AS
insert into jyang_workspace.commission
        ( commission_batch_id
          , program_name
          , user_id
          , account_id
          , lead_id
          , dw_encrypted_name
          , task_id
          , user_guid
          , first_financial_advisor_name
          , financial_advisor_name
          , is_terminated
          , comm_date
          , comm_term_date
          , term_date
          , term_month_end_date
          , comm_type
          , period
          , emp_role
          , pay_to_id
          , pay_to_name
          , sa_meet_set_created_date
          , sa_meet_set_created_by_id
          , sa_meet_set_created_by_name
          , fa_meet_held_created_date
          , fa_meet_held_created_by_id
          , fa_meet_held_created_by_name
          , fa_meet_held_type
          , base_rate
          , bonus_rate
          , total_rate
          , base_amt
          , redshift_created_date
          , redshift_updated_date
        )
     select $1
          , v.program_name
          , v.user_id
          , v.account_id
          , v.lead_id
          , null
          , v.task_id
          , v.user_guid
          , v.first_financial_advisor_name
          , v.financial_advisor_name
          , v.is_terminated
          , v.comm_date
          , v.comm_term_date
          , v.term_date
          , last_day(v.term_date) term_month_end_date
          , v.comm_type
          , v.period
          , v.emp_role
          , v.sa_meet_set_act_by_id pay_to_id
          , v.sa_meet_set_act_by_name pay_to_name
          , v.sa_meet_set_act_date
          , v.sa_meet_set_act_by_id
          , v.sa_meet_set_act_by_name
          , v.fa_meet_held_act_date
          , v.fa_meet_held_act_by_id
          , v.fa_meet_held_act_by_name 
          , v.fa_meet_held_type
          , v.base_rate                                  
          , v.bonus_rate
          , v.total_rate
          , 25 base_amt
          , sysdate
          , sysdate
        from (
             select  /*+ ordered */
                  t.account_id
                , null lead_id
                -- , a.dw_encrypted_name
                , xc.program_name
                , t.task_id
                , a.user_id
                , a.user_guid
                , a.first_fin_advisor_assign_name first_financial_advisor_name
                , a.owner_id financial_advisor_id
                , a.owner_name financial_advisor_name
                , decode(a.investment_closed_term_date, null, 0, 1) is_terminated
                , add_months(last_day(trunc(t.activity_date)) + 1, xc.add_months) comm_date
                , add_months(last_day(trunc(t.activity_date)) + 1, xc.add_months) comm_term_date
                , trunc(a.investment_closed_term_date) term_date
                , xc.comm_type
                , xc.period
                , xc.emp_role
                , t2.activity_date sa_meet_set_act_date
                -- override meeting set data for api user to owner
                , t2.created_by_id sa_meet_set_act_by_id
                , t2.created_by_name sa_meet_set_act_by_name
                , t.activity_date fa_meet_held_act_date
                , t.created_by_id fa_meet_held_act_by_id
                , t.created_by_name fa_meet_held_act_by_name
                , t.type fa_meet_held_type
                , xc.base_rate                                  
                , xc.bonus_rate
                , base_rate +  xc.bonus_rate total_rate
                , row_number() over (partition by t.account_id order by t.activity_date desc, t2.activity_date desc) sort_seq
            from salesforce_schema.task t 
            join salesforce_schema.task t2 on t.account_id = t2.account_id and  t.task_id <> t2.task_id and t2.type = 'SA Meeting Set' 
                    and t2.created_by_name <> 'api user'
            join salesforce_schema.account a on t.account_id = a.account_id and is_test = 0
            cross join jyang_workspace.commission_config xc
            where t.type in ('SA Held - Info Gathering', 'SA Held', 'SA Meeting Held', 'SA Held - In Person Info Gathering')
            and t.activity_date >= add_months($2, -1)
            and xc.comm_type = 'sa-meeting-set-fa-ig-held' and xc.emp_role = 'sa'
            union all
            select  /*+ ordered */
                  'lead-id-' || l.lead_id account_id  -- filler to keep account id unique
                , l.lead_id lead_id
                -- , l.dw_encrypted_name
                , xc.program_name
                , t.task_id
                , null user_id
                , null
                , null first_financial_advisor_name
                , l.owner_id financial_advisor_idt
                , l.owner_name financial_advisor_name
                , 0 is_terminated
                , add_months(last_day(trunc(t.activity_date)) + 1, xc.add_months) comm_date
                , add_months(last_day(trunc(t.activity_date)) + 1, xc.add_months) comm_term_date
                , null term_date
                , xc.comm_type
                , xc.period
                , xc.emp_role
                , t2.activity_date sa_meet_set_act_date
                -- override meeting set data for api user to owner
                , t2.created_by_id sa_meet_set_act_by_id
                , t2.created_by_name sa_meet_set_act_by_name
                , t.activity_date fa_meet_held_act_date
                , t.created_by_id fa_meet_held_act_by_id
                , t.created_by_name fa_meet_held_act_by_name
                , t.type fa_meet_held_type
                , xc.base_rate                                  
                , xc.bonus_rate
                , base_rate +  xc.bonus_rate total_rate
                , row_number() over (partition by t.who_id order by t.created_date desc, t2.created_date desc) sort_seq
            from salesforce_schema.task t 
            join salesforce_schema.task t2 on t.who_id = t2.who_id and  t.task_id <> t2.task_id and t2.type = 'SA Meeting Set' 
                  and t2.created_by_name <> 'api user'
            join salesforce_schema.lead l on t.who_id = l.lead_id
            cross join jyang_workspace.commission_config xc
            where t.type in ('SA Held - Info Gathering', 'SA Held', 'SA Meeting Held')
            and t.activity_date >= add_months($2, -1)
            and xc.comm_type = 'sa-meeting-set-fa-ig-held' and xc.emp_role = 'sa'            
            ) v
        left outer join jyang_workspace.commission comm on v.account_id = comm.account_id and  comm.comm_type = 'sa-meeting-set-fa-ig-held' 
                        and v.comm_date = comm.comm_date and comm.emp_role = 'sa' and comm.status = 'final' AND comm.commission_batch_id = $1
        left outer join jyang_workspace.commission comm2 on v.lead_id = comm2.lead_id and comm2.comm_type = 'sa-meeting-set-fa-ig-held' 
                        and v.comm_date = comm2.comm_date and comm2.emp_role = 'sa' and comm2.status = 'final' AND comm2.commission_batch_id = $1
        where v.sort_seq = 1
        and comm.account_id is null
        and comm2.lead_id is null
        and v.comm_date <= trunc(sysdate)
        and v.comm_date >= $2; 
        
 /* * * *
  *
  * Load base amounts (AUM) for initials
  *
  * * * * */
PREPARE prep_load_aum_detail (bigint) AS
insert into jyang_workspace.COMMISSION_AUM_DETAIL
        (commission_batch_id
        , user_id
        , comm_date
        , snapshot_date
        , user_account_id
        , raw_balance
        , redshift_created_date)
        select -- /*+ index(uas user_account_snapshot_idx1 ) */ 
         $1
        , com.user_id
        , com.comm_date
        , uas.snapshot_date
        , uas.user_account_id
        , raw_balance
        , sysdate
        from (
            select 
              com.user_id
            , com.comm_date
            from jyang_workspace.commission com
            where comm_date <= comm_term_date
            and comm_date < trunc(sysdate)
            and com.commission_batch_id = $1
            and com.initial_trading_date is not null
            group by commission_batch_id, com.user_id, com.comm_date
        ) com
        join sp_schema.user_account_snapshot uas on com.user_id = uas.user_id 
        and com.comm_date - 1 = uas.snapshot_date 
        and uas.snapshot_interval = 0 
        join sp_schema.user_account ua on uas.user_account_id = ua.user_account_id and uas.snapshot_date >= ua.initial_trading_date
        and ua.is_onus = 1;

 /* * * *
  *
  * Update AUM for fa initials
  *
  * * * * */
PREPARE prep_update_FAAUM_Balances (bigint) AS
update jyang_workspace.commission 
   set base_amt = source.raw_balance
  from jyang_workspace.commission target,
       (select 
        commission_batch_id, 
        user_id
        , comm_date
        , sum(raw_balance) raw_balance
        from jyang_workspace.commission_aum_detail
        where commission_batch_id = $1
        group by user_id, comm_date, commission_batch_id
       ) source 
         where  target.commission_batch_id = source.commission_batch_id and
                target.user_id = source.user_id and
                target.comm_type = 'initial-funding' and
                target.comm_date = source.comm_date and
                target.emp_role = 'fa';
                
 /* * * *
  *
  * Update AUM for sa initials
  *
  * * * * */
PREPARE prep_update_SAAUM_Balances (bigint) AS
update jyang_workspace.commission 
   set base_amt = source.raw_balance
  from jyang_workspace.commission target,
       (select commission_batch_id
        , user_id
        , comm_date
        , sum(raw_balance) raw_balance
        from jyang_workspace.commission_aum_detail
        where commission_batch_id = $1
        group by user_id, comm_date, commission_batch_id
        ) source
 where  target.commission_batch_id = source.commission_batch_id and
        target.user_id = source.user_id and
        target.comm_type = 'initial-funding' and
        target.comm_date = source.comm_date and
        target.emp_role = 'sa';

 /* * * *
  *
  * Calculate the commissions
  *
  * * * * */
PREPARE prep_calc_Commissions (bigint) AS
update jyang_workspace.commission 
   set comm_base_rate_amt = round(base_amt * base_rate,2)
     , comm_bonus_rate_amt = round(base_amt * bonus_rate,2)
     , comm_total_amt = round(base_amt * total_rate,2) 
 where commission_batch_id = $1;

 /* * * *
  *
  * Add terminations for fa emp_role
  * 
  * Missing aum_initial_trading_by_user.investment_closed_term_date, use salesforce_schema.account
  * * * * */
PREPARE prep_load_Terminations (bigint, DATE) AS
insert into jyang_workspace.commission
        ( commission_batch_id
          , program_name
          , account_id
          , dw_encrypted_name
          , user_id
          , user_guid
          , is_terminated
          , initial_trading_date
          , initial_trading_month_end_date
          , comm_date
          , comm_term_date
          , term_date
          , term_month_end_date
          , comm_type
          , period
          , emp_role
          , comm_base_rate_amt
          , comm_bonus_rate_amt
          , comm_total_amt
        )
        select 
            $1
          , program_name
          , account_id
          , dw_encrypted_name
          , user_id
          , user_guid
          , is_terminated
          , initial_trading_date
          , initial_trading_month_end_date
          , comm_date
          , comm_term_date
          , term_date
          , term_month_end_date
          , comm_type
          , period
          , emp_role
          , comm_base_rate_amt
          , comm_bonus_rate_amt
          , comm_total_amt
        from (
                select 
                  max(x.program_name) program_name
                , x.account_id
                , max(dw_encrypted_name) dw_encrypted_name
                , x.user_id
                , max(x.user_guid) user_guid
                , max(x.is_terminated)  is_terminated
                , max(x.initial_trading_date) initial_trading_date
                , max(last_day(x.initial_trading_date)) initial_trading_month_end_date
                , max(last_day(aum.investment_closed_term_date)+1) comm_date
                , max(aum.investment_closed_term_date) comm_term_date
                , max(aum.investment_closed_term_date) term_date
                , max(last_day(aum.investment_closed_term_date)) term_month_end_date
                , max(months_between(last_day(aum.investment_closed_term_date), initial_trading_month_end_date)) months_between_trade_and_term
                , max(case when months_between(last_day(aum.investment_closed_term_date), initial_trading_month_end_date) <=6 then 'termination-1-6'
                       else 'termination-7-12' end) comm_type
                , -1 period
                , max(emp_role) emp_role
                , sum(decode(comm_base_rate_amt,0,0,comm_base_rate_amt)) comm_base_rate_amt
                , sum(decode(comm_bonus_rate_amt,0,0,comm_bonus_rate_amt*case when months_between(last_day(aum.investment_closed_term_date), initial_trading_month_end_date) <=6 then -1 else -.5 end)) comm_bonus_rate_amt
                , sum(decode(comm_total_amt,0,0,comm_total_amt* case when months_between(last_day(aum.investment_closed_term_date), initial_trading_month_end_date) <=6 then -1 else -.5 end)) comm_total_amt
                from jyang_workspace.commission x 
                join  salesforce_schema.account aum on x.user_id = aum.user_id
                where x.redshift_id in (
                    select x.redshift_id
                    from jyang_workspace.commission x 
                    join salesforce_schema.account aum on x.user_id = aum.user_id
                    where aum.investment_closed_term_date >= add_months($2,-1) -- offset for start comm date to look at last month
                    and aum.investment_closed_term_date < add_months(last_day(trunc(sysdate)),-1)+1
                    and x.comm_type not like 'termination%'
                    and aum.investment_closed_term_date - x.initial_trading_date < 366
                    and x.comm_date < trunc(sysdate)
                    and emp_role = 'fa'
                    and x.comm_type = 'initial-funding'
                    and (lower(x.status) = 'final' )
                 ) group by x.user_id, x.account_id
            );
            
 /* * * *
  *
  * Set pay to for fa emp_role
  *
  * * * * */
PREPARE prep_set_PayTo_For_FA (bigint) AS
UPDATE jyang_workspace.commission
   SET user_guid = source.user_guid
        , first_financial_advisor_name = source.first_financial_advisor_name
        , financial_advisor_name = source.financial_advisor_name
        , financial_advisor_id = source.financial_advisor_id
        , opportunity_owner_name = source.opportunity_owner_name
        , opportunity_owner_id = source.opportunity_owner_id
        , pay_to_name = CASE WHEN source.pay_to_name LIKE '%CFP' THEN source.pay_to_name ELSE replace(source.pay_to_name,source.pay_to_name, source.pay_to_name||' CFP') END 
		--, pay_to_name = replace(source.pay_to_name,source.pay_to_name, source.pay_to_name||' CFP')
        , pay_to_id = source.pay_to_id
        , pcg_financial_advisor_id = source.pcg_financial_advisor
        , pcg_financial_advisor_name = source.pcg_financial_advisor_name
  FROM jyang_workspace.commission target,
         (select com.redshift_id
                , a.account_id
                , a.user_guid
                , a.first_fin_advisor_assign_name first_financial_advisor_name
                , a.owner_id financial_advisor_id
                , a.owner_name financial_advisor_name
                , oppor.first_invest_closed_trading_date
                , oppor.first_invest_closed_trading_date_by_id
                , oppor.opportunity_owner_id
                , oppor.opportunity_owner_name
                , oppor.opportunity_owner_id pay_to_id
                , oppor.opportunity_owner_name pay_to_name
                , a.pcg_financial_advisor
                , u.user_name pcg_financial_advisor_name
            from jyang_workspace.commission com
            join salesforce_schema.account a on com.user_id = a.user_id
            left join salesforce_schema.user u on a.pcg_financial_advisor = u.user_id
            left join  (select account_id
                          , first_invest_closed_trading_date
                          , first_invest_closed_trading_date_by_id
                          , owner_id opportunity_owner_id
                          , owner_name opportunity_owner_name
                    from (
                    select a.user_id,
                       o.account_id
                       , o.first_invest_closed_trading_date
                       , o.first_invest_closed_trading_date_by_id
                       , o.owner_id
                       , o.owner_name
                       , row_number()
                       over (partition by o.account_id order by o.first_invest_closed_trading_date, o.created_date nulls last) sort_seq
                    from salesforce_schema.opportunity o
                    join salesforce_schema.account a on o.account_id = a.account_id
                    where o.is_deleted = 0
                    and first_invest_closed_trading_date is not null                    
                    ) where sort_seq = 1
                   ) oppor on a.account_id = oppor.account_id 
                    where emp_role = 'fa'
                    and com.commission_batch_id = $1 
        ) source 
        where target.redshift_id = source.redshift_id;

 /* * * *
  *
  * Set payto override
  *
  * * * * */
PREPARE prep_set_PayTo_Override (bigint) AS
 update jyang_workspace.commission
    set pay_to_name = 'Joel Karacozoff',
        pay_to_id = (select max(user_id) from salesforce_schema.user u where u.user_name = 'joel.karacozoff@personalcapital.com'),
        financial_advisor_name = 'Joel Karacozoff'
  where commission_batch_id = $1
    and user_id in (32594,3508,621332,599265,593834,554644,521064,624794,605653,504924,490415,503316,484562,
              484844,493374,395023,510117,522081,504835,489367,522856,563910,541231,356053,501780,582860,
              574693,546008,541512,534575,598574,489944,600244,152053,562037,602989,573817,548088,621143,
              627035,20002,653440,631436,614134,539534,522699,198337,611595,590194,600140,633426,561203,599490)
    and program_name = 'Financial Advisor - Fundings';

-- Joel Karacozoff no longer gets commissions on additions after '2014-06-01'
PREPARE prep_exception_Updates_01 (bigint) AS
delete from jyang_workspace.commission
where commission_batch_id = $1
and additional_last_day_tran_date is not null
and user_id in (32594,3508,621332,599265,593834,554644,521064,624794,605653,504924,490415,503316,484562,
                  484844,493374,395023,510117,522081,504835,489367,522856,563910,541231,356053,501780,582860,
                  574693,546008,541512,534575,598574,489944,600244,152053,562037,602989,573817,548088,621143,
                  627035,20002,653440,631436,614134,539534,522699,198337,611595,590194,600140,633426,561203,599490)
and additional_last_day_tran_date>= to_date('2014-06-01', 'YYYY-MM-DD');

-- Scott Walker override
-- per ticket xxxx 
PREPARE prep_exception_Updates_02 (bigint) AS       
delete from jyang_workspace.commission
where commission_batch_id = $1
and comm_type like 'additions-year%'
and comm_date >= to_date('2016-02-01', 'YYYY-MM-DD')
and pay_to_name = 'Scott Walker';
        
-- Scott Walker max commissions overall
PREPARE prep_exception_Updates_03 (bigint) AS       
delete from jyang_workspace.commission 
where commission_batch_id = $1
and comm_date >= to_date('2017-02-01', 'YYYY-MM-DD')
and pay_to_name = 'Scott Walker';

-- Mike Hefty override
PREPARE prep_exception_Updates_04 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and comm_type like 'additions-year%'
and comm_date >= to_date('2016-07-01', 'YYYY-MM-DD')
and pay_to_name = 'Mike Hefty';

-- Mike Hefty max commissions overall
PREPARE prep_exception_Updates_05 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and comm_date >= to_date('2017-07-01', 'YYYY-MM-DD')
and pay_to_name = 'Mike Hefty';

PREPARE prep_exception_Updates_06 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and comm_date >= to_date('2017-07-01', 'YYYY-MM-DD')
and pay_to_name = 'Mike Hefty';

-- Michelle Brownstein max initial 
PREPARE prep_exception_Updates_07 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Michelle Brownstein'
and comm_type = 'initial-funding'
and initial_trading_date >= to_date('2016-09-01', 'YYYY-MM-DD');

-- Michelle Brownstein max overall
PREPARE prep_exception_Updates_08 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Michelle Brownstein'
and comm_date >= to_date('2017-09-01', 'YYYY-MM-DD');

-- Amin Dabit max initial 
PREPARE prep_exception_Updates_09 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Amin Dabit'
and comm_type = 'initial-funding'
and initial_trading_date >= to_date('2017-01-01', 'YYYY-MM-DD');

-- Amin Dabit max overall
PREPARE prep_exception_Updates_10 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Amin Dabit'
and comm_date >= to_date('2018-02-01', 'YYYY-MM-DD');
        
-- Andrew Thompson  max initial
PREPARE prep_exception_Updates_11 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Andrew Thompson'
and comm_type = 'initial-funding'
and initial_trading_date >= to_date('2017-03-01', 'YYYY-MM-DD');

-- Andrew Thompson  max overall
PREPARE prep_exception_Updates_12 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Andrew Thompson'
and comm_date >= to_date('2018-02-01', 'YYYY-MM-DD');
 
-- Bryan Lennon  max initial
PREPARE prep_exception_Updates_13 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Bryan Lennon'
and comm_type = 'initial-funding'
and initial_trading_date >= to_date('2017-04-01', 'YYYY-MM-DD');

-- Bryan Lennon  max overall
PREPARE prep_exception_Updates_14 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Bryan Lennon'
and comm_date >= to_date('2018-06-01', 'YYYY-MM-DD');
 
-- Garrett Gunberg max initial
PREPARE prep_exception_Updates_15 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Garrett Gunberg'
and comm_type = 'initial-funding'
and initial_trading_date >= to_date('2017-08-01', 'YYYY-MM-DD');

-- Garrett Gunberg max overall
PREPARE prep_exception_Updates_16 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Garrett Gunberg'
and comm_date >= to_date('2018-09-01', 'YYYY-MM-DD');

-- Adam Mazzaro max initial
PREPARE prep_exception_Updates_17 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Adam Mazzaro'
and comm_type = 'initial-funding'
and initial_trading_date >= to_date('2017-08-01', 'YYYY-MM-DD');

-- Adam Mazzaro max overall
PREPARE prep_exception_Updates_18 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Adam Mazzaro'
and comm_date >= to_date('2018-09-01', 'YYYY-MM-DD');
 
-- Whitney Pappas max initial
PREPARE prep_exception_Updates_19 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Whitney Pappas'
and comm_type = 'initial-funding'
and initial_trading_date >= to_date('2017-08-01', 'YYYY-MM-DD');

-- Whitney Pappas max overall
PREPARE prep_exception_Updates_20 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Whitney Pappas'
and comm_date >= to_date('2018-09-01', 'YYYY-MM-DD');
 
-- Michael Eichinger max initial
PREPARE prep_exception_Updates_21 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Michael Eichinger'
and comm_type = 'initial-funding'
and initial_trading_date >= to_date('2017-08-01', 'YYYY-MM-DD');

-- Michael Eichinger max overall
PREPARE prep_exception_Updates_22 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_name = 'Michael Eichinger'
and comm_date >= to_date('2018-09-01', 'YYYY-MM-DD');
    
PREPARE prep_exception_Updates_23 (bigint) AS 
delete from jyang_workspace.commission
where commission_batch_id = $1
and pay_to_name = 'Ryan Koenig' 
and comm_date >= to_date('2017-08-01', 'YYYY-MM-DD');

PREPARE prep_exception_Updates_24 (bigint) AS 
delete from jyang_workspace.commission
where commission_batch_id = $1
and pay_to_name = 'Ryan Koenig' 
and comm_date >= to_date('2017-08-01', 'YYYY-MM-DD');

-- Current Financial Advisor USER ID's of Dan, Jeff and Kyle
PREPARE prep_exception_Updates_25 (bigint) AS 
delete from jyang_workspace.commission
where commission_batch_id = $1
and financial_advisor_id in ('005F0000002Bn36IAC'
                            ,'005A0000001H54PIAS'
                            ,'005A0000000r8anIAA');
        
--Pay to Names of TERMINATED Financial Advisors
PREPARE prep_exception_Updates_26 (bigint) AS 
delete from jyang_workspace.commission 
where commission_batch_id = $1
and pay_to_id in ('005A0000001H54PIAS'
                 ,'005F0000002BaOMIA0'
                 ,'005F0000002Bn36IAC'
                 ,'005F0000002CN9qIAG'
                 ,'005F00000036ePbIAI'
                 ,'005F00000036eQKIAY'
                 ,'005F0000003bhPtIAI'
                 ,'005F0000003f3Q5IAI'
                 ,'005F0000003rUODIA2'
                 ,'005F00000045BfMIAU'
                 ,'005F00000045Cy7IAE'
                 ,'005F000000468FRIAY'
                 ,'005F00000046bgaIAA'
                 ,'005F00000046CiQIAU'
                 ,'005F00000046o1EIAQ'
                 ,'005F00000046VtKIAU'
                 ,'005F00000047izxIAA'
                 ,'005F0000006na4GIAQ'
                 ,'005F00000089jrTIAQ'
                 ,'005A0000001H54PIAS'
                 ,'005F0000002Bn36IAC'
                 ,'005F00000045BfMIAU'
                 ,'005F00000046CiQIAU'
                 ,'005F00000047izxIAA');
                 
PREPARE prep_set_Status_To_Final (bigint) AS 
update jyang_workspace.commission 
   set status = 'final'
 where commission_batch_id = $1;


execute prep_load_FA_InitialTrading (f_get_commission_batch_id(), f_get_commission_month());

execute prep_load_FA_CashAdditions (f_get_commission_batch_id(), f_get_commission_month());

execute prep_store_audit_data (f_get_commission_batch_id());

execute prep_load_SA_InitialTrading (f_get_commission_batch_id(), f_get_commission_month());

execute prep_load_SA_MeetingsSetWithIG (f_get_commission_batch_id(), f_get_commission_month());

execute prep_load_aum_detail (f_get_commission_batch_id());

execute prep_update_FAAUM_Balances (f_get_commission_batch_id());

execute prep_update_SAAUM_Balances (f_get_commission_batch_id());

execute prep_calc_Commissions (f_get_commission_batch_id());

execute prep_load_Terminations (f_get_commission_batch_id(), f_get_commission_month());

execute prep_set_PayTo_For_FA (f_get_commission_batch_id());

execute prep_set_PayTo_Override (f_get_commission_batch_id());

execute prep_exception_Updates_01 (f_get_commission_batch_id());
execute prep_exception_Updates_02 (f_get_commission_batch_id());
execute prep_exception_Updates_03 (f_get_commission_batch_id());
execute prep_exception_Updates_04 (f_get_commission_batch_id());
execute prep_exception_Updates_05 (f_get_commission_batch_id());
execute prep_exception_Updates_06 (f_get_commission_batch_id());
execute prep_exception_Updates_07 (f_get_commission_batch_id());
execute prep_exception_Updates_08 (f_get_commission_batch_id());
execute prep_exception_Updates_09 (f_get_commission_batch_id());
execute prep_exception_Updates_10 (f_get_commission_batch_id());
execute prep_exception_Updates_11 (f_get_commission_batch_id());
execute prep_exception_Updates_12 (f_get_commission_batch_id());
execute prep_exception_Updates_13 (f_get_commission_batch_id());
execute prep_exception_Updates_14 (f_get_commission_batch_id());
execute prep_exception_Updates_15 (f_get_commission_batch_id());
execute prep_exception_Updates_16 (f_get_commission_batch_id());
execute prep_exception_Updates_17 (f_get_commission_batch_id());
execute prep_exception_Updates_18 (f_get_commission_batch_id());
execute prep_exception_Updates_19 (f_get_commission_batch_id());
execute prep_exception_Updates_20 (f_get_commission_batch_id());
execute prep_exception_Updates_21 (f_get_commission_batch_id());
execute prep_exception_Updates_22 (f_get_commission_batch_id());
execute prep_exception_Updates_23 (f_get_commission_batch_id());
execute prep_exception_Updates_24 (f_get_commission_batch_id());
execute prep_exception_Updates_25 (f_get_commission_batch_id());
execute prep_exception_Updates_26 (f_get_commission_batch_id());

execute prep_set_Status_To_Final (f_get_commission_batch_id());

/*
--delete jyang_workspace.COMMISSION;

--delete jyang_workspace.COMMISSION_ADD_FUNDS_DETAIL;

select * from jyang_workspace.commission_add_funds_detail;

select * from jyang_workspace.COMMISSION_config;

*/

-- 930
select * from jyang_workspace.commission_add_funds_detail;

-- 23662
select count(1) from jyang_workspace.COMMISSION;

select * from jyang_workspace.COMMISSION;

select * from jyang_workspace.COMMISSION;
