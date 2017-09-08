#!/usr/bin/python2.7
import csv
import re
import pymysql
import subprocess
import sys, getopt
import sshtunnel
import getpass

#STARTING VARIABLES
 #field = int(sys.argv[1])# 641167
  #choice = int(sys.argv[2])
##print(CallSessionID)

#FUNCTIONS
def write_from_sql(cursor,file_i):
  with open(file_i, 'w') as fout:
    print(fout)
    writer = csv.writer(fout)
    writer.writerow([ i[0] for i in cursor.description ]) # heading row
    writer.writerows(cursor.fetchall())

def writeSt(cursor,file_o,path2script=0,args=0):
  with open(file_o, 'w') as fout:
    
    print('Saving output: ' + file_o)
    writer = csv.writer(fout)
    writer.writerow([ i[0] for i in cursor.description ]) # heading row
    writer.writerows(cursor.fetchall())
  command = 'Rscript'
  #print(args)
  #print('\n')
  cmd = [command, path2script] + args
  print('R output:\n')
  subprocess.check_output(cmd, universal_newlines=True)
  
  
def writeSt2(input_1,output_1):
  spamReader = csv.DictReader(open(input_1), delimiter=',', quotechar='"')
  contacted = []
  available = []
  called = []
  ignored_unallocated = []
  busy_on_phone = []
  #get data from  Data
  for row in spamReader:
    lines=row["Data"]
    #CONTACTED ENGINEERS
    contacted_engineers=re.compile(r'.*Calling engineer\(s\):(.*)\s(\(.*\))')
    if contacted_engineers.match(lines):
      output = contacted_engineers.sub(r'\g<1>',lines)
      called.append(output)
      #print(output)
      #print(line for line in output)
    output3=[]
    #engineers available
    available_engineers=re.compile(r'E.*-\s{(.*)}.*Busy/C.*')
    if available_engineers.match(lines):
      output2=available_engineers.sub(r'\g<1>',lines)
      output2=re.findall(r'[A-z]+\s[A-z]+',output2)
      output2.sort();
      available.append(output2)
      #print(Busy/On Phone:\n",output2)
    #busy on the phone  
    available_engineers=re.compile(r'.*Busy/O.*{([A-z]+\s[A-z]+.*)}\W<hr />Ignored.*')
    if available_engineers.match(lines):
      output2=available_engineers.sub(r'\g<1>',lines)
      output2=re.findall(r'[A-z]+\s[A-z]+',output2)
      output2.sort();
      busy_on_phone.append(output2)

      #print(output2)
      
    available_engineers=re.compile(r'.*<hr />Ignored UNALLOCATED numbers:.*]\W(.*).*<hr />C.*')
    if available_engineers.match(lines):
      output2=available_engineers.sub(r'\g<1>',lines)
      output2=re.findall(r'[A-z]+\s[A-z]+',output2)
      output2.sort();
      ignored_unallocated.append(output2)
      #print("Ignored UNALLOCATED numbers:\n",output2)
    available_engineers=re.compile(r'.*<hr />Currently contacted.*]\W{(.*)}.*')
    if available_engineers.match(lines):
      output2=available_engineers.sub(r'\g<1>',lines)
      output2=re.findall(r'[A-z]+\s[A-z]+',output2)
      output2.sort();
      contacted.append(output2)
      

  output = open(output_1, 'w')
  fnames = ['Engineers_Called', 'Available_Engineers', 'Busy_On_Phone','Ignored_unallocated']
  writer = csv.DictWriter(output, fieldnames=fnames) 
  writer.writeheader()
  for col in fnames:
    writer.writerow({
    'Engineers_Called' : '%s' % (called.pop() if len(called) != 0 else None ),
    'Available_Engineers' : '%s' % (available.pop() if len(available) != 0 else None ),
    'Busy_On_Phone' : '%s' % (busy_on_phone.pop() if len(busy_on_phone) != 0 else None), 
    'Ignored_unallocated' : '%s' % (ignored_unallocated.pop() if len(ignored_unallocated) != 0 else None)
    })
    
def sql_runn(cursor,sql,file_o,args,file_i=None,path2script=None):
  cursor.execute(sql)
  if(path2script!=None):
    writeSt(cursor,file_o,path2script,args)
  elif (file_i!=None):
    write_from_sql(cursor,file_i)
    writeSt2(file_i,file_o)
  elif(file_o!=None):
    write_from_sql(cursor,file_o)


#SQL CALL CHOICES
def sql_queries(choice,field=None,i_sql=None,file_o=None):
  args=None
  path2script=None
  file_i=None
  CallSessionID=field
  if (choice == 0):
    print("Getting Status Changes Of All Engineers\n 30 mins before CallSessionID: %s was Bridged\n\n" % CallSessionID)
    sql = '''
    select concat(lg.FirstName,' ', lg.LastName) as "User",lh.Status as "Engineer Status", ad.Name, lh.Created as "Status Change Time"
    from auth.LOGIN_AVAILABILITY_HISTORY lh 
    inner join auth.LOGIN lg on (lh.LoginID = lg.LoginID)
    inner join auth.INTERNAL_LOGIN il on lh.LoginID = il.LoginID
    inner join auth.LOGIN_AVAILABILITY la on il.LoginAvailabilityID = la.LoginAvailabilityID
    inner join auth.DEPARTMENT ad on (il.DepartmentID = ad.DepartmentID)
    inner join auth.SHIFT sh on (il.ShiftID = sh.ShiftID) where lg.Username like '%' and ad.Name like 'Applications%' 
    and  lh.Created between date_sub((select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = {varb}) ,
    interval 30 minute) and (select BridgeDate from IPcc.CallSession cs where cs.CallSessionID = {varb})  
    group by lh.Created''' .format(varb=CallSessionID) 
    file_o = "/home/amcdowald/wrksite/Metrics/outputs/Status_Change_%s.csv" % (CallSessionID)
    path2script = '/home/amcdowald/wrksite/Metrics/asa_call_data2.R'
    args = ['/home/amcdowald/wrksite/Metrics/outputs/Status_Change_%s.csv' % (CallSessionID),'%s' % (CallSessionID)]
  elif(choice==1):
    print("Getting All Calls to Engineers\n 30 mins before CallSessionID: %s Was Bridged\n\n" % CallSessionID)
    sql = """select l.Username, cs.StartDate, cs.EndDate,cs.CallSessionID, (select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = {varn}) as CallSession_Queued_Date from IPcc.CallSession cs left join auth.CLIENT c on cs.ClientID = c.clientID left join IPcc.CallHotline ch on cs.CallHotlineID = ch.CallHotlineID left join auth.LOGIN l on cs.LoginID = l.loginID left  join auth.INTERNAL_LOGIN ail on (ail.LoginID = cs.LoginID) left join auth.DEPARTMENT ad on (ad.DepartmentID = ail.DepartmentID) left join auth.OFFICE ao on (ail.OfficeID = ao.OfficeID) where cs.StartDate between date_sub((select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = {varn}),interval 30 minute) and (select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = {varn}) and ad.Name like  'Applications Management' """.format(varn=CallSessionID)
    file_o = '/home/amcdowald/wrksite/Metrics/outputs/Calls_During_%s.csv' % (CallSessionID)
    path2script = '/home/amcdowald/wrksite/Metrics/asa_call_data.R'
    args = ['/home/amcdowald/wrksite/Metrics/outputs/Calls_During_%s.csv' % (CallSessionID),'%s' % (CallSessionID) ]
  elif (choice == 2):
    print("Getting All IPcc history for CallSessionID: %s\n\n" % CallSessionID)
    sql = """select cs.CallSessionID, lg.Username ,cl.Data,cl.EventDate, 
    cs.ClientID, cs.CallerPhoneNumber, cs.LoginID, cs.QueuedDate, cs.Status,
    lh.Status, lg.Username, ad.Name, lh.Created,cl.CallSessionID
    from auth.LOGIN_AVAILABILITY_HISTORY lh 
    inner join auth.LOGIN lg on (lh.LoginID = lg.LoginID)
    inner join IPcc.CallSession cs on lg.LoginID = cs.LoginID
    inner join auth.INTERNAL_LOGIN il on cs.LoginID = il.LoginID
    inner join auth.LOGIN_AVAILABILITY la on il.LoginAvailabilityID = la.LoginAvailabilityID
    inner join auth.DEPARTMENT ad on (il.DepartmentID = ad.DepartmentID)
    inner join auth.SHIFT sh on (il.ShiftID = sh.ShiftID)
    inner join IPcc.CallLog cl on (cl.CallSessionID=cs.CallSessionID) where cs.CallSessionID like '{varb}' 
    and lg.Username like '%' and ad.Name like 'Applications%' and (cl.Data like '%Calling%' or cl.Data like'Engineers%') group by cl.Data
    """.format(varb=CallSessionID) 
    file_i = '/home/amcdowald/wrksite/Metrics/outputs/Data_%s.csv' % (CallSessionID)
    file_o = '/home/amcdowald/wrksite/Metrics/outputs/Log_History_CallID:%s.csv' % (CallSessionID)
  elif(choice == 4):
    sql = ''' select  
   all_tickets.Client,
   all_tickets.Type    as 'Ticket Type',
   all_tickets.IMTID as 'IPim TID',
   all_tickets.TID     as 'Radar TID',
   all_tickets.MTID    as 'Master TID',
   if(all_tickets.automaton_count > 0, 'Yes', 'No')  as 'Automation',
   all_tickets.`Correlated (IPsec)` as 'IPsec Correlated',
   all_tickets.`Master Ticket`      as 'Master Ticket',
   all_tickets.`Correlated Tickets` as 'Correlated Ticket Count',
   all_tickets.`Ticket_Updates`     as 'Update Count',
   all_tickets.`Handoffs`           as 'Handoff Count',
   all_tickets.Owner                as 'Owner',
   all_tickets.Creator              as 'Creator',
   all_tickets.Priority             as 'Priority',
   all_tickets.Subject              as 'Subject',
   all_tickets.initial_department   as  'Initial Department',
   all_tickets.Department           as 'Pickup Department',
   all_tickets.TicketCreated        as 'Ticket Created (UTC)',
   all_tickets.Queue                as 'Ticket Queue',
   all_tickets.RadarQueue           as 'Initial Radar Queue',
   
 if(all_tickets.RespondTime is null, 
		'Null', all_tickets.RespondTime/60)     as "Respond Time",
   
   all_tickets.MTTResolve           as "Resolve Time",   
   if(all_tickets.RespondTime/60 >= 15, "Yes", "No") as "Miss SLA",  
   
   ifnull(
      (select ShiftID
       from auth.SHIFT
       where HOUR(all_tickets.action) >= startHour and HOUR(all_tickets.action) < endHour limit 1
      ),2
   ) as ShiftID,
   all_tickets.ATT   

from (
   select
	   c.ClientClientname as 'Client',
	   im.ipim_id as 'IMTID',
	   t.ticket_id as 'TID',
	   t.master_ticket_id as "MTID",
	   mon.ipmon_host as "IPmon Host",	

       if(t.master_ticket_id is not null AND ipsec.ticket_id is not null, 'Yes','No') as "Correlated (IPsec)",
       if(t.master_ticket_id is null AND ipsec.ticket_id is not null,'Yes','No') as "Master Ticket",
       if(t.master_ticket_id is null AND ipsec.ticket_id is not null, (
          select count(*) from IPradar.tickets ts
	      where ts.master_ticket_id = TID
	     ),0
	   ) as "Correlated Tickets",
		(SELECT
		  CASE TRUE
			WHEN ((SELECT
				COUNT(ticket_id)
			  FROM IPradar.ipcal_ticket_mapping
			  WHERE ticket_id = im.ticket_id
               )
			  ) THEN "IPCAL"
			WHEN ((SELECT
				COUNT(ticket_id) Bla
			  FROM IPradar.rfc_ticket_mapping
			  WHERE ticket_id = im.ticket_id)
			  ) THEN "CHNG"
			WHEN ((SELECT
				COUNT(ticket_id)
			  FROM IPpm.problem
			  WHERE ticket_id = im.ticket_id)
			  ) THEN "IPPM"
			WHEN (locate("project", LOWER(iq.description)) AND
			  (ISNULL(im.ticket_id))) THEN "PRJ"
			WHEN NOT ISNULL(mon.ticket_id) THEN "IPMON"
			ELSE "IPIM"
		  END
         ) as Type,

	   (select count(*) from 
		 IPautomata.execution ipaut 
		 where ipaut.ticket_id = t.ticket_id
		) as 'automaton_count', 
		
 	   (SELECT COUNT(tr.id) AS Updates
		FROM IPim.Tickets t
		LEFT JOIN IPim.Transactions tr ON t.id = tr.objectid
		LEFT JOIN IPim.Attachments a ON tr.id = a.transactionid
		WHERE t.id = im.ipim_id
		AND tr.type IN ('Correspond', 'Create')
		AND tr.objecttype = 'RT::Ticket'      
	   ) as "Ticket_Updates",
	   (SELECT
		  COUNT(tr.id) AS Handoffs
		FROM IPim.Tickets t
		LEFT JOIN IPim.Transactions tr
		  ON t.id = tr.objectid
		LEFT JOIN IPim.Attachments a
		  ON tr.id = a.transactionid
		WHERE t.id = im.ipim_id
		AND tr.type = 'Give'
		AND tr.objecttype = 'RT::Ticket'       
       ) as 'Handoffs', 
	   concat(ow.FirstName, ' ', ow.LastName) as "Owner",
	   cr.RealName as "Creator",
	   ifnull(concat('P', t.criticality_id), "POther") as "Priority",
	   t.description as 'Subject',
	   d.Name as 'Department',
	   t.create_date as TicketCreated,
	   iq.name as "Queue",
	   ipsec.ticket_id as "IPsecTid",	   
    
	   TIME_TO_SEC(TIMEDIFF((select max(tl.action_date) from IPradar.ticket_log tl where tl.ticket_id=t.ticket_id and tl.status_id =1),t.create_date)) as 'RespondTime',
	   (select ts.time_to_resolution/60 from IPslr.ticket_summary ts where ts.ticket_id = t.ticket_id) as MTTResolve,
	   (select max(tl.action_date) from IPradar.ticket_log tl where tl.ticket_id=t.ticket_id and tl.status_id =1) as action,
       
		(select iq.name
         from IPradar.ticket_log a 
         use key (idx_ticket_log_ticket_action)
		 left join IPradar.queue iq on (iq.queue_id = a.queue_id) 
         where a.ticket_id = t.ticket_id 
 		 order by a.action_date ASC limit 1 )  as 'RadarQueue',
		
		(select create_date from IPradar.ticket_log where ticket_id =t.ticket_id limit 1) as 'radar_create_date',
		(select min(tl.action_date)	from IPradar.ticket_log tl
			where tl.ticket_id=t.ticket_id  and tl.action_description in
			('User Ticket Pickup', 'User Ticket Update', 'IPautomata Update')
		) as 'user_picked_date',
		(select max(tl.action_date)	 from IPradar.ticket_log tl
			where tl.ticket_id=t.ticket_id and tl.status_id=1
		) as 'status_new_date',
		(select max(tl.action_date)	 from IPradar.ticket_log tl
			where tl.ticket_id=t.ticket_id and
		tl.status_id = 6) as 'resolved_date',       
              
	    (select logdept.name 
         from auth.DEPARTMENT logdept 
		 where logdept.DepartmentID=(
            select tlog.department_id 
			from IPradar.ticket_log tlog 
			where tlog.ticket_id=t.ticket_id and tlog.department_id IS NOT NULL
			order by update_date ASC limit 1
         )
        ) as 'initial_department',
	   (select tlog.department_id from IPradar.ticket_log tlog where tlog.ticket_id=t.ticket_id and tlog.department_id IS NOT NULL
 	    order by update_date ASC limit 1) as 'non-newdept',
	   CONCAT('S',IFNULL((select ShiftID from auth.SHIFT where HOUR(t.create_date) >= startHour and HOUR(t.create_date) < endHour limit 1), 2)) as Shift,
       (CASE WHEN c.PortalThemeID = 3 THEN "Yes" ELSE "No" END) as 'ATT'

	from IPradar.tickets t
		inner join IPradar.ipim_ticket_mapping im on im.ticket_id=t.ticket_id
	    left join IPradar.ipmon_ticket_mapping mon on (mon.ticket_id = t.ticket_id and mon.state != "UNKNOWN")
		left join IPradar.rfc_ticket_mapping cm on cm.ticket_id=t.ticket_id
		left join IPradar.project_ticket_mapping pj on pj.ticket_id=t.ticket_id
		left join IPpm.problem pm on pm.ticket_id=t.ticket_id
		left join IPradar.ipsec_ticket_mapping ipsec on ipsec.ticket_id=t.ticket_id		

		inner join auth.CLIENT c on t.client_id=c.clientid
		left join auth.LOGIN l on t.owner_id=l.loginid
		left join auth.LOGIN ow on (t.owner_id = ow.LoginID)

		left join IPim.Tickets it on (im.ipim_id = it.id)
		left join IPim.Users cr on (it.Creator = cr.id)
		left join IPim.Queues iq on (iq.id = it.Queue)

		left join auth.INTERNAL_LOGIN il on l.LoginID=il.LoginID
		left join auth.DEPARTMENT d on il.DepartmentID=d.DepartmentID
		left join auth.OFFICE ao on (il.OfficeID = ao.OfficeID)		
		inner join IPradar.status s on s.status_id=t.status_id

	where
		t.create_date >= curdate() - INTERVAL DAYOFWEEK(curdate())+6 DAY AND
		t.create_date  < curdate() - INTERVAL DAYOFWEEK(curdate())-1 day AND
		ipsec.ticket_id is null AND
		not lower(iq.name) REGEXP '.*project.*'

Union 

		select
	    c.ClientClientname as 'Client',
	    im.ipim_id as 'IMTID',
	    tr.ticket_id as 'TID',
	    tr.master_ticket_id as "MTID",
		mon.ipmon_host as "IPmon Host",
        
	    -- was this resolved ticket correlated by IPsec?
	    if(tr.master_ticket_id is not null AND ipsec.ticket_id is not null, 'Yes','No') as "Correlated (IPsec)",
	    if(tr.master_ticket_id is null AND ipsec.ticket_id is not null,'Yes','No') as "Master Ticket",
	    if(tr.master_ticket_id is null AND ipsec.ticket_id is not null,
		    (select count(*) 
		     from IPradar.tickets_resolved t
		     where t.master_ticket_id = TID
		    ),
		    0
	    ) as "Correlated Tickets",
		
		(SELECT
		  CASE TRUE
			WHEN ((SELECT
				COUNT(ticket_id)
			  FROM IPradar.ipcal_ticket_mapping
			  WHERE ticket_id = im.ticket_id
			  -- AND ISNULL(mon.ticket_id)
			  -- AND cr.RealName like '%IPcal%'
               )
			  ) THEN "IPCAL"
			WHEN ((SELECT
				COUNT(ticket_id) Bla
			  FROM IPradar.rfc_ticket_mapping
			  WHERE ticket_id = im.ticket_id)
			  ) THEN "CHNG"
			WHEN ((SELECT
				COUNT(ticket_id)
			  FROM IPpm.problem
			  WHERE ticket_id = im.ticket_id)
			  ) THEN "IPPM"
			WHEN (locate("project", LOWER(iq.description)) AND
			  (ISNULL(im.ticket_id))) THEN "PRJ"
			WHEN NOT ISNULL(mon.ticket_id) THEN "IPMON"
			ELSE "IPIM"
		  END
         ) as Type,
		                 
	   (select count(*) from IPautomata.execution ipaut where ipaut.ticket_id = tr.ticket_id) as 'automaton_count', 
	   (SELECT
		  COUNT(tr.id) AS Updates
		FROM IPim.Tickets t
		LEFT JOIN IPim.Transactions tr
		  ON t.id = tr.objectid
		LEFT JOIN IPim.Attachments a
		  ON tr.id = a.transactionid
		WHERE t.id = im.ipim_id
		AND tr.type IN ('Correspond', 'Create')
		AND tr.objecttype = 'RT::Ticket'      
	   ) as "Ticket_Updates",
	   (SELECT
		  COUNT(tr.id) AS Handoffs
		FROM IPim.Tickets t
		LEFT JOIN IPim.Transactions tr
		  ON t.id = tr.objectid
		LEFT JOIN IPim.Attachments a
		  ON tr.id = a.transactionid
		WHERE t.id = im.ipim_id
		AND tr.type = 'Give'
		AND tr.objecttype = 'RT::Ticket'       
       ) as 'Handoffs',

		concat(l.FirstName,' ', l.LastName) as "Owner",
	    cr.RealName as "Creator",
	    ifnull(concat('P', tr.criticality_id),"POther") as "Priority",

		tr.description as 'Subject',
	    d.Name as 'Department',
	    tr.create_date as 'TicketCreated',
	    iq.name as "Queue",	   
	    ipsec.ticket_id as "IPsecTid",
        
        
	    ts.time_to_respond as 'RespondTime',
        
		(select ts.time_to_resolution/60 from IPslr.ticket_summary ts where ts.ticket_id = tr.ticket_id) as MTTResolve,
	    
	    (select max(tl.action_date) 
	     from IPradar.ticket_log tl 
	     where tl.ticket_id=tr.ticket_id and tl.status_id =1
	    ) as 'action',
	                    
 		(select iq.name
         from IPradar.ticket_log a 
         use key (idx_ticket_log_ticket_action)
		 left join IPradar.queue iq on (iq.queue_id = a.queue_id) 
         where a.ticket_id = tr.ticket_id 
 		 order by a.action_date ASC limit 1 )  as 'RadarQueue',

		(select create_date from IPradar.ticket_log where ticket_id =tr.ticket_id limit 1) as 'radar_create_date',
		(select min(tl.action_date)	from IPradar.ticket_log tl
			where tl.ticket_id=tr.ticket_id  and tl.action_description in
			('User Ticket Pickup', 'User Ticket Update', 'IPautomata Update')
		) as 'user_picked_date',
		(select max(tl.action_date)	 from IPradar.ticket_log tl
			where tl.ticket_id=tr.ticket_id and tl.status_id=1
		) as 'status_new_date',
		(select max(tl.action_date)	 from IPradar.ticket_log tl
			where tl.ticket_id=tr.ticket_id and
		tl.status_id = 6) as 'resolved_date',
   
		(select logdept.name 
         from auth.DEPARTMENT logdept 
		 where logdept.DepartmentID=(
            select tlog.department_id 
			from IPradar.ticket_log tlog 
			where tlog.ticket_id=tr.ticket_id and tlog.department_id IS NOT NULL
			order by update_date ASC limit 1
         )
        ) as 'initial_department',

	    (select tlog.department_id 
	     from IPradar.ticket_log tlog 
	     where tlog.ticket_id=tr.ticket_id and tlog.department_id IS NOT NULL
	     order by update_date ASC limit 1
	    ) as 'non-newdept',
		CONCAT('S',IFNULL((select ShiftID from auth.SHIFT where HOUR(tr.create_date) >= startHour and HOUR(tr.create_date) < endHour limit 1), 2)) as Shift,
	    (CASE WHEN c.PortalThemeID = 3 THEN "Yes" ELSE "No" END) as 'ATT'

	from IPradar.tickets_resolved tr
		inner join IPradar.ipim_ticket_mapping im on im.ticket_id=tr.ticket_id
		left join IPradar.ipmon_ticket_mapping mon on (mon.ticket_id = tr.ticket_id and mon.state != "UNKNOWN")
		left join IPradar.rfc_ticket_mapping cm on cm.ticket_id=tr.ticket_id
		left join IPradar.project_ticket_mapping pj on pj.ticket_id=tr.ticket_id
		left join IPradar.ipsec_ticket_mapping ipsec on ipsec.ticket_id=tr.ticket_id
		left join IPpm.problem pm on pm.ticket_id=tr.ticket_id
		
		inner join auth.CLIENT c on c.ClientID=tr.client_id
		left join auth.LOGIN l on tr.owner_id=l.LoginID
		left join auth.INTERNAL_LOGIN il on l.LoginID=il.LoginID
	
		left join auth.OFFICE ao on (il.OfficeID = ao.OfficeID)
		left join IPim.Tickets it on (im.ipim_id = it.id)
		left join IPim.Users cr on (it.Creator = cr.id)
		left join IPim.Queues iq on (iq.id = it.Queue)

		left join IPslr.ticket_summary ts on ts.ticket_id=tr.ticket_id
		left join auth.INTERNAL_LOGIN ail on (ail.LoginID = ts.top_ipsoft_poster_id)
		left join auth.DEPARTMENT d on tr.department_id=d.DepartmentID
		inner join IPradar.status s on s.status_id=tr.status_id

	where
		tr.create_date >= curdate() - INTERVAL DAYOFWEEK(curdate())+6 DAY AND
		tr.create_date  < curdate() - INTERVAL DAYOFWEEK(curdate())-1 day AND
		cr.realname != "IPsoft IPcal" AND
		 not lower(iq.name) REGEXP '.*project.*'
) all_tickets
where 
	all_tickets.Type  in ('IPMON','CHNG','IPIM') and 
    all_tickets.`initial_department`  not in (
      'Cognitive Division', 
	  'Research & Development', 
      'Service Transition', 
      'Quality Assurance', 
      'Security',
	  'Business',
	  'Human Resources',
      'Sales',
      'Finance & Accounting',
	  'Communications',
      'Legal',
      'Marketing',
	  'Office Management')'''  
    file_o = field
  elif(choice == 5):
    sql = i_sql
    file_o = '{infi}'.format(infi=file_o)
  return sql, file_o, args, file_i, path2script

def get_password():
  pass_wd = getpass.getpass("Enter your password: ")
  return pass_wd


#MAIN
def sql_calls(choice,field=None,i_sql=None,file_o=None):
  #pass_wd = get_password()
  sql=i_sql
  _host = "produtil01.ipsoft.ny1"
  _ssh_port = 22
  _username = 'amcdowald'
  _password = "2Chron48"#pass_wd
  _local_bind_address = '0.0.0.0'
  _local_mysql_port = 12345
  _remote_bind_address = "ipdb-s"
  _remote_mysql_port = 3306
  with sshtunnel.SSHTunnelForwarder(
    (_host, _ssh_port),
    ssh_username=_username,
    ssh_password=_password,
    remote_bind_address=(_remote_bind_address, _remote_mysql_port),
    local_bind_address=(_local_bind_address, _local_mysql_port)
    ) as tunnel:
    connection = pymysql.connect(host='localhost',
			      port=12345,
			      user='readonly',
			      password='read0nly',
			      db='IPradar'
			    )
    try:
      with connection.cursor() as cursor:
	sql,file_o,file_i,path2script,args = sql_queries(choice,field,i_sql,file_o)
	sql_runn(cursor,sql,file_o,file_i,path2script,args)
    finally:
      connection.close()
