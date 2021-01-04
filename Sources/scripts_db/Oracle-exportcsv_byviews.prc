create or replace function exportcsv_byviews(powner varchar2,pviewnamelike varchar2,pcostlimited number default 5000000) return varchar2
/*
   Description: using for AutomaticQuerySystemBaseOnEMail
    Created by: xlzhu@ips.com
Creattion time: 201810
    Updated by: xlzhu add default parameter pcostlimited
  Updated time: 201812
*/
is
v_sql varchar2(256);
v_vcnt number(4);
v_cost number;
cursor cur_views is select owner,object_name as view_name,substr(object_name,1,length(object_name)-1) as file_name from dba_objects 
                    where owner=powner and object_type like 'VIEW' and object_name like 'UID%FILE%V' and object_name like pviewnamelike and status='VALID' 
					and last_ddl_time>sysdate-1/24 order by owner,object_name;
begin
  if upper(powner)='SYS' or upper(powner)='SYSTEM' then
    dbms_output.put_line('forbid owner:'||powner);
  else
    if pcostlimited>0 then
      dbms_output.put_line('GetViewExplainPlan');
      for c1 in cur_views loop
        v_sql:='explain plan for select * from '||c1.owner||'.'||c1.view_name;
        execute immediate v_sql;
        v_sql:='select cost from plan_table where timestamp>sysdate-1/144 and plan_id=(select max(plan_id) from plan_table) and id=0 and rownum=1';
        execute immediate v_sql into v_cost;
        rollback;
        if v_cost>pcostlimited then
          return 'explain_plan_too_bad(cost>'||to_char(pcostlimited)||'),sql_order:'||substr(c1.view_name,instr(c1.view_name,'_',1,2)+1,instr(c1.view_name,'V')-instr(c1.view_name,'_',1,2)-1);
        end if;
      end loop;
    end if;
    dbms_output.put_line('ExportCSVDropView');
    v_vcnt:=0;
    for c1 in cur_views loop
      v_vcnt:=v_vcnt+1;
      SYS.exportcsv_forbigdata(p_viewname=>c1.owner||'.'||c1.view_name,p_filename=>c1.file_name,p_dir=>'DMP_AUTOQUERY',p_wnullfile=>True);
      v_sql:='drop view '||c1.owner||'.'||c1.view_name;
      dbms_output.put_line(v_sql);
      execute immediate v_sql;
    end loop;
  end if;
  dbms_output.put_line('view processed cnt:'||to_char(v_vcnt));
  return 'OK';
end;
/
