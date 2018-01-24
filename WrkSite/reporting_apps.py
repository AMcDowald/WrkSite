#!/usr/bin/python3
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
import pandas as pd
import getpass
import re
import operator
from os.path import isfile, join
from os import listdir
import numpy as np
import sys

class database_connections:
    def __init__(self, data_base):
        self.data_base = data_base
        # self._password = self.get_password()
        if self.data_base == 'SHARED':
            self._host = "produtil01.ipsoft.ny1"
            self._ssh_port = 22
            self._username = 'amcdowald'
            self._password = self.get_password()
            self._local_bind_address = '0.0.0.0'
            self._local_mysql_port = 22345
            self._remote_bind_address = "ipdb-s"
            self._remote_mysql_port = 3306
            self._sql_username = 'readonly'
            self._sql_password = 'read0nly'
            self._sql_db_name = 'IPradar'
        if self.data_base == 'LAB':
            self._host = "10.60.101.64"
            self._ssh_port = 22
            self._username = 'amcdowald'
            self._password = '2Chron48'  # self.get_password()
            self._local_bind_address = '0.0.0.0'
            self._local_mysql_port = 12345
            self._remote_bind_address = "127.0.0.1"
            self._remote_mysql_port = 3306
            self._sql_username = 'root'
            self._sql_password = 'root'
            self._sql_db_name = 'MetricsDB'
        if self.data_base == 'LAB_LOCAL':
            self._local_mysql_port = 3306
            self._sql_username = 'root'
            self._sql_password = 'root'
            self._sql_db_name = 'MetricsDB'

    def create_sql_engine(self):
        if self.data_base == 'LAB_LOCAL':
            engine = create_engine('mysql+pymysql://%s:%s@127.0.0.1:%s/%s' % (
                self._sql_username, self._sql_password, self._local_mysql_port, self._sql_db_name))
            return engine
        else:
            print("CONNECTED TO DATABASE: %s" % self.data_base)
            tunnel = SSHTunnelForwarder(
                (self._host, self._ssh_port),
                ssh_username=self._username,
                ssh_password=self._password,
                remote_bind_address=(self._remote_bind_address, self._remote_mysql_port),
                local_bind_address=(self._local_bind_address, self._local_mysql_port))
            tunnel.start()
            engine = create_engine('mysql+pymysql://%s:%s@127.0.0.1:%s/%s' % (
                self._sql_username, self._sql_password, tunnel.local_bind_port, self._sql_db_name))
            engine = engine.execution_options(autocommit=True, autoflush=False, expire_on_commit=False)
        return tunnel, engine

    def df_sql_shared(self, start_date, end_date):
        # year,month,date '2017-02-01'
        self.start_date = start_date
        self.end_date = end_date
        shared_sql = """select
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
          		t.create_date BETWEEN DATE('{date}') AND DATE('{end_date}') AND
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
          			  -- AND cr.RealName like '%%IPcal%%'
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
          		tr.create_date BETWEEN DATE('{date}') AND DATE('{end_date}') AND
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
          	  'Office Management')""".format(date=start_date, end_date=end_date)
        tunnel, engine = self.create_sql_engine()
        query_output = pd.read_sql_query(shared_sql, engine)
        tunnel.close()
        return query_output

    def get_password(self):
        self.pass_wd = getpass.getpass("Enter your password for %s: " % (self.data_base))
        return self.pass_wd


class create_megalog:
    def __init__(self, team, IN_FOLDER, OUT_FOLDER):
        self.IN_FOLDER = IN_FOLDER
        self.OUT_FOLDER = OUT_FOLDER
        self.team = team
        self.rawMetrics_location = '%sRaw/' % self.IN_FOLDER
        self.rawMetrics_outPut_location = '%s' % self.OUT_FOLDER
        if re.match('.*Apps.*', self.team):
            self.rawMetrics_file_name = 'Application_Tlog_Megalog'
            self.rawMetrics_team_name = '(IPsoft) Applications Management|Applications Management'
        if re.match('.*Ent.*', self.team):
            self.rawMetrics_file_name = 'Enterprise_Tlog_Megalog'
            self.rawMetrics_team_name = 'Enterprise Applications|(IPsoft) Enterprise Applications'

    def read_data(self):
        dataframe = pd.read_excel(
            io='%s%s.xlsx' % (self.rawMetrics_outPut_location, self.rawMetrics_file_name),
            sheet_name='%s' % self.rawMetrics_file_name, na_values=['Null', 'null'],
            dtype={'Subject': str, 'Respond Time': np.float64, 'Resolve Time': np.float64,
                   'Miss SLA': str})

        return dataframe

    def create_tlog(self):
        self.rawMetricsSearch_sheet_name = 'TLog'
        DATAFRAME = self.WRT_createMegalog(self.rawMetrics_location, self.rawMetrics_outPut_location,
                                           self.rawMetrics_file_name,
                                           self.rawMetrics_team_name, self.rawMetricsSearch_sheet_name)
        return DATAFRAME

    # changes to
    def WRT_createMegalog(self, rawMetrics_location, outPut_location, file_name, team_name, sheet_name):
        raw_dataframe = self.DFM_make_files_into_data_frame('%s' % rawMetrics_location, sheet_name)
        dataframe = self.SHP_by_Team(raw_dataframe, team_name)
        dataframe = self.SHP_datetime(dataframe)
        # dataframe= self.SHP_CLIENT(dataframe)
        self.WRT_dataframe(dataframe, outPut_location, file_name)
        return dataframe

    def DFM_make_files_into_data_frame(self, file_location, sheet_name):
        list_of_files = [file for file in functions.getfiles(file_location)]
        if list_of_files:
            try:

                data_frame_date = {}
                # MOD FOR
                [operator.setitem(data_frame_date, num,
                                  pd.read_excel(file_address, sheet_name='%s' % sheet_name, index_col=None,
                                                na_values=['Null', 'null'],
                                                dtype={'Subject': str, 'Respond Time': np.float64,
                                                       'Resolve Time': np.float64,
                                                       'Miss SLA': str})) for
                 num, file_address in enumerate(list_of_files)]

                # THIS>># map(lambda file_adr: operator.__setitem__(data_frame_date,file_adr[0],pd.read_excel(file_adr[1],sheet_name='%s'%sheet_name,index_col=None)),[file_address for file_address in enumerate(list_of_files)])
                DATAFRAME_DICTIONARY = pd.concat(data_frame_date, ignore_index=True)
                return DATAFRAME_DICTIONARY
            except Exception as Exception_Message:
                print("Create DataFrame has failed: %s" % Exception_Message)
                return 1
        else:
            sys.exit("DIRECTORY \"%s\" IS EMPTY" % file_location)

    def SHP_by_Team(self, dataframe, team_name):
        shp_dataframe = dataframe[dataframe['Initial Department'].str.contains(r'%s' % team_name)]
        return shp_dataframe

    def SHP_datetime(self, dataframe):
        # series_1 = pd.to_datetime(dataframe['Ticket Created (UTC)']).dt.date.rename('Date')
        series_2 = pd.to_datetime(dataframe['Ticket Created (UTC)']).dt.weekday_name.rename('Day')
        series_3 = pd.to_datetime(dataframe['Ticket Created (UTC)']).dt.round('H').dt.strftime('%H:%M').rename('Hour')
        SHP_DF = pd.concat([series_2, series_3, dataframe], axis=1)
        return SHP_DF

    def SHP_CLIENT(self, dataframe):
        WITHOUT_MASTERCARD = ['ATOS-MHFI', 'HO', 'Verizon', 'UAL', 'Celesio', 'cognizant', 'nssol', 'ndivision', 'PwC',
                              'NE', 'KTR',
                              'MasterCard', 'TFSSG', 'JNDATA', 'BT-Interserve', 'IPsoft', 'AtosUK', 'BlackRock', 'CGI',
                              'CSH', 'Equens', 'GMOne', 'GPM', 'IBS', 'JWT', 'THD', 'THDCA', 'THDNS', 'TSysBP',
                              'didata',
                              'fits']
        clients = '|'.join(WITHOUT_MASTERCARD)
        shp_dataframe = dataframe[~dataframe['Client'].str.contains(r'%s' % clients)]
        # print(shp_dataframe)
        return shp_dataframe

    def WRT_dataframe(self, dataframe, outPut_location, file_name):
        writer = pd.ExcelWriter('%s%s.xlsx' % (outPut_location, file_name))
        dataframe.to_excel(writer, '%s' % file_name, index=False)
        writer.save()
        writer.close()

        # def getfiles(self, mypath):
        #     onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        #     names = []
        #     for i_file in onlyfiles:
        #         names.append('{path}{i_file}'.format(path=mypath, i_file=i_file))
        #     return names

class pivot:
    def pvt_attendance(self, writer, data_frame_dict_per_shift, sheet_name):
        start_row = 0
        # writer = pd.ExcelWriter('%s' % OUTPUT_FILE, engine='xlsxwriter')
        position_data = {}
        for date in data_frame_dict_per_shift.items():
            date[1].to_excel(writer, sheet_name='%s' % sheet_name, startcol=0, startrow=start_row)
            end_row = start_row + date[1].shape[0]
            position_data['%s' % date[1].index.names[0]] = {'start_row': start_row, 'rows': date[1].shape[0],
                                                            'columns': date[1].shape[1], 'end_row': end_row}
            start_row = start_row + date[1].shape[1] + 10
        return position_data

    # Create Pivots
    # PIE PIVOTS

    # VOL
    def pvt_pie1_vol_by_client_other(self, dataframe, writer, sheet_name, position_tuple):
        df = dataframe['Client'].value_counts()
        df = df.to_frame(name='Volume')
        total = df.sum().values[0]
        df = df.fillna(value=0)
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return total, df

    def pvt_pie2_vol_by_shift_client_other(self, dataframe, writer, sheet_name, position_tuple):
        df = dataframe[['Client', 'ShiftID']].groupby(['ShiftID']).count()
        total = df.sum().values[0]
        df = df.fillna(value=0)
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return total, df

    def pvt_pie3_vol_by_shift_client_other(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[dataframe.Automation.str.contains("No") == False]
        df = dataframe[['Automation', 'Client']].groupby(['Client']).count()
        total = df.sum().values[0]
        df = df.fillna(value=0)
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return total, df

    def pvt_pie4_vol_by_shift_client_other(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[dataframe.Automation.str.contains("Yes") == False]
        df = dataframe[['Automation', 'Client']].groupby(['Client']).count()
        total = df.sum().values[0]
        df = df.fillna(value=0)
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return total, df

    def pvt_pie5_vol_by_shift_client_other(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[['Automation', 'Client']]
        df = pd.pivot_table(dataframe, index=dataframe.Automation, values='Client', aggfunc='count')
        df = df.rename({'Yes': 'Automata Present', 'No': 'Automata Missing'})
        df = df.fillna(value=0)
        total = ''
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return total, df

    # MISS
    def pvt_pie1_miss_sla_client_other(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[dataframe.Automation.str.contains("No") == False]
        dataframe = dataframe[dataframe['Miss SLA'].str.contains("No") == False]
        dataframe = dataframe.pivot_table(index='Client', values="Automation", aggfunc='count')
        dataframe.fillna(value=0)
        try:
            # print(dataframe)
            total = dataframe.Automation.sum()
        except:
            total = 0
            print('no data')
        dataframe.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return total, dataframe

    def pvt_pie2_miss_sla_client_other(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[dataframe['Miss SLA'].str.contains("No") == False]
        dataframe = dataframe.pivot_table(index='ShiftID', values='Miss SLA', aggfunc='count')
        try:
            total = dataframe['Miss SLA'].sum()
        except:
            total = 0
            print('no data')
        dataframe.fillna(value=0)
        dataframe.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return total, dataframe

    def pvt_pie3_miss_sla_client_other(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[dataframe['Miss SLA'].str.contains("No") == False]
        dataframe = dataframe.pivot_table(index=dataframe.Client, values='Miss SLA', aggfunc='count')

        try:
            total = dataframe['Miss SLA'].sum()
        except:
            total = 0
            print('no data')
        dataframe.fillna(value=0)
        dataframe.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return total, dataframe

    def pvt_pie4_miss_sla_by_client_ex_mc(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[['Miss SLA', 'Client', 'IPim TID', 'Date']]
        dataframe = dataframe.pivot_table(index=['Miss SLA'], columns=['Client'], values=['IPim TID'], aggfunc=len)
        # print(dataframe)
        dataframe.columns = dataframe.columns.droplevel()
        try:
            total = dataframe.sum()[0]  # make into function
        except:
            total = 0
            print('no data')
        dataframe.fillna(value=0)
        dataframe.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])

        return total, dataframe

    # COLUMN
    def pvt_col1_eng_auto_miss_vol(self, dataframe, writer, sheet_name, position_tuple):
        result = dataframe.set_index([dataframe.Owner.str.contains('.*auto.*|.*prod.*')])
        # print(result)
        df1 = result.pivot_table(index=['Date', 'Day'], values=['IPim TID'], aggfunc=len)
        df1.columns = ['Total Volume' for i in df1.columns]
        df1 = df1.fillna(value=0)
        result = result[result['Miss SLA'].str.contains('Yes')]
        df = result.set_index([result.Owner.str.contains('.*auto.*|.*prod.*')])
        # df.Date = pd.to_datetime(df.Date.values, format='%m-%d-%Y').date
        df = result.pivot_table(index=['Date', 'Day'], columns=df.index, values=['Miss SLA'], aggfunc=len)
        df.columns = map(lambda tup: '%s' % str(
            'Engineer Tickets' if tup[0] == 'IPim TID' else "Engineer Misses") if tup[
                                                                                      1] == False else 'Automata %s' % str(
            'Tickets' if tup == 'IPim TID' else 'Misses'), [(i, j) for i, j in df.columns])
        df = df1.merge(df, left_index=True, right_index=True, how='right')
        df = df.fillna(value=0)
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return df

    def pvt_col2_eng_auto_miss_vol(self, dataframe, writer, sheet_name, position_tuple):
        result = dataframe
        result = result.set_index([result.Owner.str.contains('.*auto.*|.*prod.*')])
        df1 = result.pivot_table(index=['Date', 'Day'], values=['IPim TID'], aggfunc=len)
        df1.columns = ['Total Volume' for i in df1.columns]
        df1 = df1.fillna(value=0)
        df = result.set_index([result.Owner.str.contains('.*auto.*|.*prod.*')])
        # df.Date = pd.to_datetime(df.Date.values, format='%m-%d-%Y').date
        df = result.pivot_table(index=['Date', 'Day'], columns=df.index, values=['IPim TID'], aggfunc=len)
        df.columns = map(lambda tup: '%s' % str(
            'Engineer Tickets' if tup[0] == 'IPim TID' else "Engineer Tickets") if tup[
                                                                                       1] == False else 'Automata %s' % str(
            'Tickets' if tup[0] == 'IPim TID' else 'Tickets'), [(i, j) for i, j in df.columns])
        df = df1.merge(df, left_index=True, right_index=True, how='right')
        df = df.fillna(value=0)
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return df

    def pvt_col3_eng_auto_miss_vol(self, dataframe, writer, sheet_name, position_tuple):
        dataframe.fillna(value='No Owner')
        dataframe.set_index('Owner')
        dataframe.Owner.str.contains('.*auto.*|.*prod.*')
        tot_vol_df = dataframe.pivot_table(index=['Date', 'Day'], values=['IPim TID'], aggfunc=len)
        tot_vol_df.columns = ['Total Volume' for i in tot_vol_df.columns]
        tot_vol_df.fillna(value=0)
        dis_vol_by_shf = dataframe.pivot_table(index=['Date', 'Day'], columns=['ShiftID'], values=['IPim TID'],
                                               aggfunc=len)  # .drop('IPim TID', level=1)

        dis_vol_by_shf.columns = map(
            lambda tup: '%s' % str('Shift 1' if tup[1] == 1 else 'Shift 2' if tup[1] == 2  else 'Shift 3'),
            [(i, j) for i, j in dis_vol_by_shf.columns])
        mrgd_tot_shf_df = tot_vol_df.merge(dis_vol_by_shf, left_index=True, right_index=True, how='right')
        mrgd_tot_shf_df.fillna(value=0)
        mrgd_tot_shf_df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0],
                                 startrow=position_tuple[1])
        return mrgd_tot_shf_df

    # PURE PIVOT
    def pvt_eng_resolved(self, writer, sheet_name, dataframe, position):
        dataframe.Owner = dataframe.Owner.fillna(value='No Owner')
        result = dataframe
        workbook = writer.book

        engineer_roster = ['Aaron Williams', 'Andre Stewart', 'Anthony McDowald', 'Emmanuel Ojo', 'Erison Melo',
                           'Jasom Odoom', 'Juan Diaz', 'Lukasz Balicki', 'Michael Trifilio', 'Olliec Crenshaw',
                           'Roozbeh Adhami', 'Saveliy Matatov', 'Anish Kesavankutty', 'Venkat Rangaraju', 'Hari Mitta',
                           'Phani Surisetty', 'Harshan Kundur', 'Sairam Leelahar', 'Mutyalaiah Arcot',
                           'Mohammed Basheer',
                           'Krishna Jakkala', 'Ravi Dandi', 'Premalatha Moses', 'Sandeep Talakkilevalappil',
                           'Suresh Ummadisetti', 'Sahana Ghosh', 'Abhishek Soni', 'Mithunnath Mannil', 'Siva Juturu',
                           'Rohit Amlani', 'Upali Bhattacharjee', 'Anuj Madaan', 'Kartheek Pemma', 'Anshu Kumar',
                           'Chandresh Jain', 'Mahendranath Madhava', 'Prerna Singh', 'Neville Aquinas']
        pos = position
        result = result[result.Client.str.contains('MasterCard')]
        result = result[~result.Owner.str.contains('.*auto.*|.*prod.*')]
        df = result.pivot_table(index=['Day'], columns=result.Owner, values=['Resolve Time'], aggfunc=len)
        df = df.reindex(['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'])
        df = df.fillna(value=0)
        df = df.T
        df['Total'] = df.sum(axis=1).round(0)
        df = df.xs('Resolve Time')
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=pos[0], startrow=pos[1])
        worksheet = writer.sheets['%s' % sheet_name]
        start_row = pos[1] + 1
        start_col = pos[0] + 1
        end_row = pos[1] + df.shape[0]
        end_col = pos[0] + df.shape[1] - 1
        endrow = 5 + df.shape[0]
        # 'A6:I69'
        worksheet.add_table(5, 0, endrow, df.shape[1], {'style': 'Table Style Light 8', 'columns': [{'header': 'Owner'},
                                                                                                    {
                                                                                                        'header': 'Sunday'},
                                                                                                    {
                                                                                                        'header': 'Monday'},
                                                                                                    {
                                                                                                        'header': 'Tuesday'},
                                                                                                    {
                                                                                                        'header': 'Wednesday'},
                                                                                                    {
                                                                                                        'header': 'Thursday'},
                                                                                                    {
                                                                                                        'header': 'Friday'},
                                                                                                    {
                                                                                                        'header': 'Saturday'},
                                                                                                    {
                                                                                                        'header': 'Total'}]})
        color_red = workbook.add_format({'bg_color': '#ff0000',
                                         'font_color': '#9C0006'})
        color_green = workbook.add_format({'bg_color': '#C6EFCE',
                                           'font_color': '#006100'})
        color_yellow = workbook.add_format({'bg_color': '#ffff00',
                                            'font_color': '#9C0006'})
        color_orange = workbook.add_format({'bg_color': '#ffa500',
                                            'font_color': '#006100'})
        worksheet.conditional_format(start_row, start_col, end_row, end_col, {'type': 'cell',
                                                                              'criteria': 'between',
                                                                              'minimum': 0,
                                                                              'maximum': 20,
                                                                              'format': color_green})
        worksheet.conditional_format(start_row, start_col, end_row, end_col, {'type': 'cell',
                                                                              'criteria': 'between',
                                                                              'minimum': 21,
                                                                              'maximum': 40,
                                                                              'format': color_yellow})
        worksheet.conditional_format(start_row, start_col, end_row, end_col, {'type': 'cell',
                                                                              'criteria': 'between',
                                                                              'minimum': 41,
                                                                              'maximum': 60,
                                                                              'format': color_orange})
        worksheet.conditional_format(start_row, start_col, end_row, end_col, {'type': 'cell',
                                                                              'criteria': '>',
                                                                              'value': 60,
                                                                              'format': color_red})
        return df

    def pvt2_line_client_missed_auto_owned(self, writer, sheet_name, dataframe, position_tuple):
        result = dataframe[['Day', 'Client', 'Date', 'Miss SLA', 'Owner']]
        result = result[result['Miss SLA'].str.contains('Yes')]
        result = result[~result.Owner.str.contains('.*auto.*| .*prod.*', na=False)]  #
        df = pd.pivot_table(result, index=['Date', 'Day'], columns=['Client'], values=['Miss SLA'], aggfunc=len)
        df.columns = df.columns.droplevel()
        df = df.fillna(value=0)
        end_date = 'None'
        begin_date = 'None'
        try:
            end_date = df.index.get_level_values('Date')[len(df.index.get_level_values('Date')) - 1]
            begin_date = df.index.get_level_values('Date')[0]
        except:
            print("No Data for this!")
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return df

    @staticmethod
    def pvt_scorecard_conditional(writer, sheet_name, dataframe, position):
        workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]

        start_row = position[1]
        start_col = position[0]
        end_row = position[1] + dataframe.shape[0]
        end_col = position[0] + dataframe.shape[1] - 1
        endrow = dataframe.shape[0]
        # print(start_row, start_col, endrow, end_col)
        worksheet.add_table(start_row, start_col, endrow, end_col,
                            {'style': 'Table Style Light 8', 'columns': [{'header': 'Platform'},
                                                                         {
                                                                             'header': 'Department'},
                                                                         {
                                                                             'header': 'Score'},
                                                                         {
                                                                             'header': 'MTTRespond (Min)'},
                                                                         {
                                                                             'header': 'MTTResolve (Min)'},
                                                                         {
                                                                             'header': 'SLA Misses'},
                                                                         {
                                                                             'header': 'QoS'},
                                                                         {
                                                                             'header': 'Stale Tickets'},
                                                                         {
                                                                             'header': 'ASA (Sec)'},
                                                                         {
                                                                             'header': 'Ticket Variance'},
                                                                         {
                                                                             'header': 'ASA Variance'},
                                                                         {
                                                                             'header': 'AR%'},
                                                                         ]})
        color_red = workbook.add_format({'bg_color': '#ff0000',
                                         'font_color': '#9C0006'})
        color_green = workbook.add_format({'bg_color': '#C6EFCE',
                                           'font_color': '#006100'})
        # worksheet.conditional_format(start_row, start_col, end_row, end_col, {'type': 'cell',
        #                                                                       'criteria': 'between',
        #                                                                       'minimum': 0,
        #                                                                       'maximum': 20000,
        #                                                                       'format': color_green})

        list = [[7, '<'], [60, '>'], [60, '>'], [0, '>'], [1.80, '<'], [0, '>'], [30, '>'], [0, '>'], [0, '>'],
                [10, '<']]
        start_col = start_col + 2
        start_row = start_row + 1
        for col, value in enumerate(list):
            worksheet.conditional_format(start_row, start_col + col, end_row, start_col + col, {'type': 'text',
                                                                                                'criteria': 'containing',
                                                                                                'value': 'NaN',
                                                                                                'format': color_green})
            worksheet.conditional_format(start_row, start_col + col, end_row, start_col + col, {'type': 'cell',
                                                                                                'criteria': '%s' %
                                                                                                            value[1],
                                                                                                'value': value[0],
                                                                                                'format': color_red})
            if value[1] == '<':
                value[1] = '>='
            elif value[1] == '>':
                value[1] = '<='
            worksheet.conditional_format(start_row, start_col + col, end_row, start_col + col, {'type': 'cell',
                                                                                                'criteria': '%s' %
                                                                                                            value[1],
                                                                                                'value': value[0],
                                                                                                'format': color_green})


    # RFC
    def create_pivot_for__RFC(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[dataframe.Status.str.contains('COMP')]
        pivot_for_rfc_over_distribution = dataframe.pivot_table(index=['Shift Day'], columns=dataframe.Shift,
                                                                values=['Status'], aggfunc=len)
        pivot_for_rfc_over_distribution = pivot_for_rfc_over_distribution.reindex(
            ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'])
        pivot_for_rfc_over_distribution = pivot_for_rfc_over_distribution.fillna(value=0)
        pivot_for_rfc_over_distribution.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0],
                                                 startrow=position_tuple[1])
        return pivot_for_rfc_over_distribution

    def create_pivot_for__RFC_2(self, dataframe, writer, sheet_name, position_tuple):
        dataframe = dataframe[~dataframe.Status.str.contains('COMP')]
        pivot_for_rfc_over_distribution = dataframe.pivot_table(index=['Shift Day'], columns=dataframe.Shift,
                                                                values=['Status'], aggfunc=len)
        pivot_for_rfc_over_distribution = pivot_for_rfc_over_distribution.reindex(
            ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'])
        pivot_for_rfc_over_distribution = pivot_for_rfc_over_distribution.fillna(value=0)
        pivot_for_rfc_over_distribution.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0],
                                                 startrow=position_tuple[1])
        return pivot_for_rfc_over_distribution

    # SCORECARD
    def pvt_scorecard(self, writer, sheet_name, dataframe, position_tuple, input_val):
        dataframe['%s' % input_val] = pd.to_numeric(dataframe['%s' % input_val], errors='coerce')
        df = dataframe.pivot_table(values='%s' % input_val, index=dataframe.Date, columns='Platform')
        df = df.fillna(value=0)
        if 'BLANK' in df.columns:
            df = df.drop('BLANK', axis=1)
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        return df
    #AUTOMATA
    def pvt_automata_mc(self,writer,sheet_name,position,dataframe):
        dataframe = dataframe[['Report_Date', 'Automation', 'Client']]
        df = pd.pivot_table(dataframe, index='Report_Date', columns=dataframe.Automation, values='Client',
                            aggfunc='count')
        df = df.rename(columns={'Yes': 'Automata Present', 'No': 'Automata Missing'})
        df = df.fillna(value=0)
        df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position[0], startrow=position[1])
        return df

class create_df:
    def WRT_dataframe(self, dataframe, outPut_location, file_name):
        writer = pd.ExcelWriter('%s%s.xlsx' % (outPut_location, file_name))
        dataframe.to_excel(writer, '%s' % file_name, index=False)
        writer.save()
        writer.close()

    def WRT_createMegalog(self, rawMetrics_location, outPut_location, file_name, team_name, sheet_name):
        raw_dataframe = create_megalog.DFM_make_files_into_data_frame('%s' % rawMetrics_location, sheet_name)
        dataframe = self.SHP_by_Team(raw_dataframe, team_name)
        dataframe = self.SHP_datetime(dataframe)
        # dataframe = self.SHP_CLIENT(dataframe)
        self.WRT_dataframe(dataframe, outPut_location, file_name)
        return dataframe

    # Dataframe
    # def DFM_make_files_into_data_frame(self, file_location, sheet_name):
    #     try:
    #         list_of_files = [file for file in self.getfiles(file_location)]
    #         data_frame_date = {}
    #         # MOD FOR
    #         [operator.setitem(data_frame_date, num,
    #                           pd.read_excel(file_address, sheet_name='%s' % sheet_name, index_col=None,
    #                                         na_values=['Null', 'null'],
    #                                         dtype={'Subject': str, 'Respond Time': np.float64,
    #                                                'Resolve Time': np.float64,
    #                                                'Miss SLA': str})) for
    #          num, file_address in enumerate(list_of_files)]
    #
    #         # THIS>># map(lambda file_adr: operator.__setitem__(data_frame_date,file_adr[0],pd.read_excel(file_adr[1],sheet_name='%s'%sheet_name,index_col=None)),[file_address for file_address in enumerate(list_of_files)])
    #         DATAFRAME_DICTIONARY = pd.concat(data_frame_date, ignore_index=True)
    #     except Exception as Exception_Message:
    #         print("Create DataFrame has failed: %s" % Exception_Message)
    #     return DATAFRAME_DICTIONARY

    def DFM_Attendance(self, file_name_address):
        xl = pd.ExcelFile(file_name_address)
        total_rows = xl.book.sheet_by_index(0).nrows
        skiprows = 2
        nrows = 35
        skip_footer = total_rows - nrows - skiprows - 1
        MASTER_DF = xl.parse(0, skiprows=skiprows, index_col=[0, 1, 2], skip_footer=skip_footer, header=[0, 1]).dropna(
            axis=1,
            how='all')
        MASTER_DF.index.names = ['No', 'Names', 'Total']
        return MASTER_DF

    def DFM_RFC(self, RFC_FILE_LOC, RFC_REPORT_FILE):
        EXCEL_FILE = pd.read_excel("%s%s.xlsx" % (RFC_FILE_LOC, RFC_REPORT_FILE))
        return EXCEL_FILE

    # Shape
    def SHP_datetime(self, dataframe):
        # series_1 = pd.to_datetime(dataframe['Ticket Created (UTC)']).dt.date.rename('Date')
        series_2 = pd.to_datetime(dataframe['Ticket Created (UTC)']).dt.weekday_name.rename('Day')
        series_3 = pd.to_datetime(dataframe['Ticket Created (UTC)']).dt.round('H').dt.strftime('%H:%M').rename('Hour')
        SHP_DF = pd.concat([series_2, series_3, dataframe], axis=1)
        return SHP_DF

    def SHP_by_Team(self, dataframe, team_name):
        shp_dataframe = dataframe[dataframe['Initial Department'].str.contains(r'%s' % team_name)]
        return shp_dataframe

    def SHP_Attendance(self, file_name_address):
        MASTER_DF = self.DFM_Attendance(file_name_address)
        data_frame_dict_per_shift = {}
        for Shift in ['S1', 'S2', 'S3']:
            df = MASTER_DF.transpose().loc[(slice(None), '%s' % Shift), :]
            n = len(df.index.values)
            data_frame_date = pd.DataFrame()
            for num in range(n):
                on_time = df.iloc[num].str.contains('P', flags=re.IGNORECASE).sum()
                late_with_notification = df.iloc[num].str.contains('LN', flags=re.IGNORECASE).sum()
                late_without_notification = df.iloc[num].str.contains('LW', flags=re.IGNORECASE).sum()
                WHD_1st_half = df.iloc[num].str.contains('1H', flags=re.IGNORECASE).sum()
                WHD_2nd_half = df.iloc[num].str.contains('2H', flags=re.IGNORECASE).sum()
                OOO = df.iloc[num].str.contains('A', flags=re.IGNORECASE).sum()
                date_col = df.index[num][0].date()

                foo_df = {'On Time': on_time, 'Late (with notification)': late_with_notification,
                          'Late (without notification)': late_without_notification,
                          'Working Half Day (1st half)': WHD_1st_half, 'Working Half Day (2nd half)': WHD_2nd_half,
                          'OOO': OOO}
                new = pd.DataFrame.from_dict(foo_df, orient='index')
                new.columns = ['%s' % date_col]
                data_frame_date = pd.concat([data_frame_date, new], axis=1)
            data_frame_dict_per_shift[Shift] = data_frame_date.transpose()
            data_frame_dict_per_shift[Shift].index.names = ['%s' % Shift]
            # Make all 0 into NaN
            data_frame_dict_per_shift[Shift] = data_frame_dict_per_shift[Shift].replace(to_replace=0, value=np.nan)
        return data_frame_dict_per_shift

    def data_by_client_instance(self, dataframe):
        # Change client in reporting
        NOT_SHARED = ['ATOS-MHFI', 'HO', 'Verizon', 'IBM-GMU', 'TSYS', 'UAL', 'Celesio', 'cognizant', 'nssol',
                      'ndivision', 'PwC',
                      'NE', 'KTR', 'TFSSG', 'JNDATA', 'BT-Interserve', 'IPsoft', 'AtosUK', 'BlackRock', 'CGI',
                      'CSH', 'Equens', 'GMOne', 'GPM', 'IBS', 'JWT', 'THD', 'THDCA', 'THDNS', 'TSysBP', 'didata',
                      'fits', 'IHG', 'VNSNY', 'ATOS', 'SF', 'MasterCard']
        shared_df = dataframe[~dataframe['Client'].isin(NOT_SHARED)]
        Mastercard_df = dataframe[dataframe['Client'].str.contains('MasterCard')]
        VNSNY_df = dataframe[dataframe['Client'].str.contains('VNSNY')]
        IHG_df = dataframe[dataframe['Client'].str.contains('IHG')]
        return shared_df, Mastercard_df, VNSNY_df, IHG_df

    def score(self, dataframe):
        shared_df, Mastercard_df, VNSNY_df, IHG_df = self.data_by_client_instance(dataframe)
        shared_df


class create_graphs:
    def grp_attendance(self, writer, sheet_name, position_data):
        workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]
        graph_pos = ['H33', 'H17', 'H1']
        for Shift in position_data.keys():
            chart = workbook.add_chart({'type': 'column', 'subtype': 'stacked'})
            df_col = position_data[Shift]['columns']
            c_start_row = position_data[Shift]['start_row']
            c_end_rows = position_data[Shift]['end_row']
            for i in range(df_col):
                col = i + 1
                chart.add_series({
                    'name': ['%s' % sheet_name, c_start_row, col],
                    'categories': ['%s' % sheet_name, c_start_row + 1, 0, c_end_rows, 0],
                    'values': ['%s' % sheet_name, c_start_row + 1, col, c_end_rows, col],
                    'marker': {'type': 'diamond'},
                    'data_labels': {'value': True, 'position': 'center'},
                })
            chart.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
            chart.set_size({'x_scale': 2, 'y_scale': 1})
            # chart.set_x_axis({'position_axis': 'on_tick'})
            title = '%s Attendance Distribution' % Shift
            chart.set_style(2)
            chart.set_title({'name': '%s' % (title)})
            worksheet.insert_chart(graph_pos.pop(), chart)
            # writer.close()

    # PIE CHARTS
    def pie_vol_by_client_other(self, dataframe, writer, sheet_name, position, total, title, values=False,
                                percentage=False):
        self.workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]

        df_col = dataframe.shape[1]
        df_row = dataframe.shape[0]  # 11
        col_pos = position[0]  # 0
        row_pos = position[1]  # 5

        c_start_col = col_pos
        c_end_cols = c_start_col
        c_start_row = row_pos + 1
        c_end_rows = c_start_row + df_row - 1

        v_start_col = col_pos + 1
        v_end_cols = v_start_col + df_col - 1
        v_start_row = row_pos + 1
        v_end_rows = v_start_row + df_row - 1

        chart = self.workbook.add_chart({'type': 'pie'})
        chart.add_series({'values': [sheet_name, v_start_row, v_start_col, v_end_rows, v_end_cols],
                          'categories': [sheet_name, c_start_row, c_start_col, c_end_rows, c_end_cols],
                          'data_labels': {'percentage': percentage,
                                          'value': values,
                                          'position': 'outside_end',
                                          'font': {'color': '#000000'}
                                          },

                          })
        chart.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart.set_title({'name': '%s %s' % (title, total)})
        chart.set_style(2)
        ch_row = row_pos
        ch_col = v_end_cols + 2
        worksheet.insert_chart(ch_row, ch_col, chart)

    # COLUMNS CHARTS
    def cht_eng_auto_miss_vol1(self, dataframe, writer, sheet_name, position, title):
        workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]
        df_col = dataframe.shape[1]
        df_row = dataframe.shape[0]  # 11
        col_pos = position[0]  # 0
        row_pos = position[1]  # 5

        c_start_col = col_pos + 1
        c_end_cols = c_start_col
        c_start_row = row_pos + 1
        c_end_rows = c_start_row + df_row - 1

        v_start_col = col_pos + 1
        v_end_cols = v_start_col + df_col - 1
        v_start_row = row_pos + 1
        v_end_rows = v_start_row + df_row - 1

        chart1 = workbook.add_chart({'type': 'column'})
        for i in range(df_col - 1):
            col = i + 1
            col = col + c_start_col
            chart1.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, v_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
                'data_labels': {'value': True},

            })

        chart1.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart1.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart1.set_y_axis({'name': 'Volume'})
        chart1.set_style(2)
        chart1.set_title({'name': '%s' % (title)})

        chart2 = workbook.add_chart({'type': 'column'})
        for i in range(df_col - 1, df_col):
            col = i + 1
            col = col + c_start_col
            chart2.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, v_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
                'data_labels': {'value': True, 'position': 'inside_end'},
                'y2_axis': True,

            })

        chart2.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart2.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart2.set_y2_axis({'name': 'Missed'})
        chart2.set_style(2)
        chart2.set_title({'name': '%s' % (title)})

        chart1.combine(chart2)

        ch_row = row_pos
        ch_col = v_end_cols + 2
        worksheet.insert_chart(ch_row, ch_col, chart1, {'x_scale': 2, 'y_scale': 1})

    def cht_eng_auto_miss_vol2(self, dataframe, writer, sheet_name, position, title):
        workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]
        df_col = dataframe.shape[1]
        df_row = dataframe.shape[0]  # 11
        col_pos = position[0]  # 0
        row_pos = position[1]  # 5
        c_start_col = col_pos + 1
        c_end_cols = c_start_col
        c_start_row = row_pos + 1
        c_end_rows = c_start_row + df_row - 1

        v_start_col = col_pos + 1
        v_end_cols = v_start_col + df_col - 1
        v_start_row = row_pos + 1
        v_end_rows = v_start_row + df_row - 1

        # print(c_start_row, c_start_col,   c_end_rows, c_end_cols)
        chart1 = workbook.add_chart({'type': 'column'})
        for i in range(round(df_col / 2)):
            col = i + 1
            col = col + c_start_col
            chart1.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, v_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
                'data_labels': {'value': True},

            })

        chart1.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart1.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart1.set_y_axis({'name': 'Total Volume'})
        chart1.set_style(2)
        chart1.set_title({'name': '%s' % (title)})

        chart2 = workbook.add_chart({'type': 'column'})
        for i in range(round(df_col / 2), df_col):
            col = i + 1
            col = col + c_start_col
            chart2.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, v_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
                'data_labels': {'value': True, 'position': 'inside_end'},
                'y2_axis': True,

            })

        chart2.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart2.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart2.set_y2_axis({'name': 'Engineer & Automata Volume'})
        chart2.set_style(2)

        chart2.set_title({'name': '%s' % (title)})

        chart1.combine(chart2)

        ch_row = row_pos
        ch_col = v_end_cols + 2
        worksheet.insert_chart(ch_row, ch_col, chart1, {'x_scale': 2, 'y_scale': 1})

    def cht_eng_auto_miss_vol3(self, dataframe, writer, sheet_name, position, title):
        workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]
        df_col = dataframe.shape[1]
        df_row = dataframe.shape[0]  # 11
        col_pos = position[0]  # 0
        row_pos = position[1]  # 5
        c_start_col = col_pos + 1
        c_end_cols = c_start_col
        c_start_row = row_pos + 1
        c_end_rows = c_start_row + df_row - 1

        v_start_col = col_pos + 1
        v_end_cols = v_start_col + df_col - 1
        v_start_row = row_pos + 1
        v_end_rows = v_start_row + df_row - 1

        # print(c_start_row, c_start_col,   c_end_rows, c_end_cols)
        chart1 = workbook.add_chart({'type': 'column'})
        for i in range(df_col - 3):
            col = i + 1
            col = col + c_start_col
            chart1.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, v_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
                'data_labels': {'value': True},

            })

        chart1.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart1.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart1.set_y_axis({'name': 'Volume'})
        chart1.set_style(2)
        chart1.set_title({'name': '%s' % title})

        chart2 = workbook.add_chart({'type': 'column'})
        for i in range(df_col - 3, df_col):
            col = i + 1
            col = col + c_start_col
            chart2.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, v_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
                'data_labels': {'value': True, 'position': 'inside_end'},
                # 'y2_axis': True,

            })

        chart2.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart2.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart2.set_y2_axis({'name': 'Vol by Shift'})
        chart2.set_style(2)
        chart2.set_title({'name': '%s' % (title)})

        chart1.combine(chart2)

        ch_row = row_pos
        ch_col = v_end_cols + 2
        worksheet.insert_chart(ch_row, ch_col, chart1, {'x_scale': 2, 'y_scale': 1})

    def RFC_Distribution_Over_Week_Chart(self, dataframe, writer, sheet_name, position, title):
        workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]
        df_col = dataframe.shape[1]
        df_row = dataframe.shape[0]  # 11
        col_pos = position[0]  # 0
        row_pos = position[1]  # 5

        c_start_col = col_pos
        c_end_cols = c_start_col
        c_start_row = row_pos + 3
        c_end_rows = c_start_row + df_row - 1

        v_start_col = col_pos
        v_end_cols = v_start_col + df_col - 1
        v_start_row = row_pos + 3
        v_end_rows = v_start_row + df_row - 1

        # print(c_start_row, c_start_col,   c_end_rows, c_end_cols)
        chart = workbook.add_chart({'type': 'column'})
        for i in range(df_col):
            col = i + 1
            chart.add_series({
                'name': ['%s' % sheet_name, c_start_row - 2, col],
                'categories': ['%s' % sheet_name, c_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, v_start_row, col, v_end_rows, col],
                'marker': {'type': 'diamond'},
                'data_labels': {'value': True, 'position': 'inside_end'},
            })

        chart.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart.set_y_axis({'visible': False}, )
        chart.set_style(2)
        chart.set_title({'name': '%s' % (title)})
        ch_row = row_pos
        ch_col = v_end_cols + 2
        worksheet.insert_chart(ch_row, ch_col, chart, {'x_scale': 2, 'y_scale': 1})

    def apps_scorecard(self, dataframe, writer, sheet_name, position, title):
        workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]
        df_col = dataframe.shape[1]
        df_row = dataframe.shape[0]  # 11
        col_pos = position[0]  # 0
        row_pos = position[1]  # 5

        c_start_col = col_pos
        c_end_cols = c_start_col
        c_start_row = row_pos + 1
        c_end_rows = c_start_row + df_row - 1

        v_start_col = col_pos + 1
        v_end_cols = v_start_col + df_col - 1
        v_start_row = row_pos + 1
        v_end_rows = v_start_row + df_row - 1

        chart = workbook.add_chart({'type': 'line'})
        for i in range(df_col):
            col = i + 1
            col = col + c_start_col
            chart.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, c_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
            })

        chart.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'on_tick'})
        chart.set_style(2)
        chart.set_title({'name': '%s' % (title)})
        ch_row = row_pos
        ch_col = v_end_cols + 2
        worksheet.insert_chart(ch_row, ch_col, chart, {'x_scale': 2, 'y_scale': 1})

    def apps_week_dist(self, dataframe, writer, sheet_name, position, title):
        workbook = writer.book
        worksheet = writer.sheets['%s' % sheet_name]
        df_col = dataframe.shape[1]
        df_row = dataframe.shape[0]  # 11
        col_pos = position[0]  # 0
        row_pos = position[1]  # 5
        c_start_col = col_pos
        c_end_cols = c_start_col
        c_start_row = row_pos + 1
        c_end_rows = c_start_row + df_row - 1

        v_start_col = col_pos
        v_end_cols = v_start_col + df_col - 1
        v_start_row = row_pos + 1
        v_end_rows = v_start_row + df_row - 1
        # print(df_col)
        # print(c_start_row, c_start_col,   c_end_rows, c_end_cols)
        chart1 = workbook.add_chart({'type': 'line'})
        for i in range(df_col - 1):
            col = i + 1
            col = col + c_start_col
            chart1.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, v_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
                'data_labels': {'value': True},

            })

        chart1.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart1.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart1.set_y_axis({'name': 'Total Automata Missing'})
        chart1.set_style(2)
        chart1.set_title({'name': '%s' % title})

        chart2 = workbook.add_chart({'type': 'column'})
        for i in range(df_col - 1, df_col):
            col = i + 1
            col = col + c_start_col
            chart2.add_series({
                'name': ['%s' % sheet_name, c_start_row - 1, col],
                'categories': ['%s' % sheet_name, v_start_row, c_start_col, c_end_rows, c_end_cols],
                'values': ['%s' % sheet_name, c_start_row, col, c_end_rows, col],
                'marker': {'type': 'diamond'},
                # 'data_labels': {'value': True, 'position': 'inside_end'},
                'y2_axis': True,

            })

        chart2.set_chartarea({'gradient': {'colors': ['#808080', '#C0C0C0', '#FFFFFF']}})
        chart2.set_x_axis(
            {'name': 'Week', 'text_axis': True, 'date_axis': True, 'minor_unit': 1, 'major_unit': 7, 'interval_tick': 1,
             'position_axis': 'between'})
        chart2.set_y2_axis({'name': 'Total Automata Present'})
        chart2.set_style(2)
        chart2.set_title({'name': '%s' % (title)})
        chart1.combine(chart2)
        ch_row = row_pos
        ch_col = v_end_cols + 2
        worksheet.insert_chart(ch_row, ch_col, chart1, {'x_scale': 2, 'y_scale': 1})


class reporting(create_megalog):
    def __init__(self, team):
        # , date, team
        self.OUT_FOLDER = '/home/amcdowald/wrksite/OPs_Metrics/New/Data/out/'
        self.IN_FOLDER = '/home/amcdowald/wrksite/OPs_Metrics/New/Data/in/'
        self.RFC_REPORT_FILE = 'MC RFC Report-Dec31-Jan6-2018'
        self.attendance = 'Shared Apps Team  Attendance - Dec 31 to Jan 6 - 2017'

        self.pvt = pivot()
        self.grh = create_graphs()
        self.cdf = create_df()

        self.team = team

        # self.DATE = date#'2018-1-08'
        # create_df=create_df()
        # create_graphs=create_graphs

    def testFunc(self):
        print(self.OUT_FOLDER)
        self.file_name = 'Applications_Report'
        self.MASTER_WRITER = pd.ExcelWriter('%s%s.xlsx' % (self.OUT_FOLDER, self.file_name))
        self.Test_Charts(self.MASTER_WRITER)

    def run_reporting(self):
        if re.match('.*Apps.*', self.team):

            self.MASTER_WRITER = pd.ExcelWriter('%s%s.xlsx' % (self.OUT_FOLDER, self.file_name))
            self.list_of_reports(self.MASTER_WRITER, self.RFC_REPORT_FILE)
        if re.match('.*Ent.*', self.team):
            self.file_name = 'Enterprise_Report'
            self.MASTER_WRITER = pd.ExcelWriter('%s%s.xlsx' % (self.OUT_FOLDER, self.file_name))
            self.master_dataframe_ent = create_megalog(self.team, self.IN_FOLDER, self.OUT_FOLDER).read_data()
            series_1 = pd.to_datetime(self.master_dataframe_ent['Ticket Created (UTC)']).dt.date.rename('Date')
            self.master_dataframe_ent = pd.concat([series_1, self.master_dataframe_ent], axis=1)
            # self.Column_Charts(self.MASTER_WRITER, self.master_dataframe_ent)
            self.PVT_Charts(self.MASTER_WRITER, self.master_dataframe_ent, self.team)
            self.Volume_Charts_Pie(self.MASTER_WRITER, self.master_dataframe_ent)
            self.apps_chart_reports(self.MASTER_WRITER)
        self.MASTER_WRITER.close()

    def list_of_reports(self, MASTER_WRITER, RFC_REPORT_FILE):
        master_dataframe = create_megalog(self.team, self.IN_FOLDER, self.OUT_FOLDER).read_data()
        # appended change
        series_1 = pd.to_datetime(master_dataframe['Ticket Created (UTC)']).dt.date.rename('Date')
        master_dataframe = pd.concat([series_1, master_dataframe], axis=1)

        shared_df, Mastercard_df, VNSNY_df, IHG_df = self.cdf.data_by_client_instance(master_dataframe)
        master_dataframe = pd.concat([shared_df, Mastercard_df, VNSNY_df, IHG_df], axis=0)

        # print(master_dataframe.head())
        # # Working Reports
        self.Volume_Charts_Pie(MASTER_WRITER, master_dataframe)
        self.Attendance_Reports(MASTER_WRITER)
        self.Column_Charts(MASTER_WRITER, Mastercard_df)
        self.RFC_Charts(MASTER_WRITER, RFC_REPORT_FILE)
        self.PVT_Charts(MASTER_WRITER, master_dataframe, self.team)
        self.apps_chart_reports(MASTER_WRITER)
        self.automata_mc_chart(MASTER_WRITER)


    def Attendance_Reports(self, MASTER_WRITER):
        print('Running Attendance Reports\n\n')
        file_name_address = '%sAttdn/%s.xlsx' % (self.IN_FOLDER, self.attendance)
        sheet_name = 'Cmb_Chart'
        # OUTPUT_FILE = '/home/amcdowald/wrksite/OPs_Metrics/outputs/Attendance_Reports.xlsx'
        try:
            df = self.cdf.SHP_Attendance(file_name_address)
            position_data = self.pvt.pvt_attendance(MASTER_WRITER, df, sheet_name)
            self.grh.grp_attendance(MASTER_WRITER, sheet_name, position_data)
        except Exception as Exception_Message:
            print("Create DataFrame has failed:\n %s\n\n" % Exception_Message)
            print("ATTENDANCE FAILED!!CHECK FOLDER\n")

    def Volume_Charts_Pie(self, MASTER_WRITER, master_dataframe):
        print('Running Volume Charts\n\n')
        # Variable
        writer = MASTER_WRITER

        Mastercard_df = master_dataframe[master_dataframe['Client'].str.contains('MasterCard')]
        NotMastercard_df = master_dataframe[~master_dataframe['Client'].str.contains('MasterCard|AtosUK|IPsoft')]

        # NOT MASTERCARD
        # VOL
        VOLUME_SHEET = "Volume"
        pvt_position_tuple = [[12, 36], [0, 36], [12, 5], [0, 5]]
        # P1
        position = pvt_position_tuple.pop()
        self.title1 = 'Ticket Volume by Client\n(Exluding MasterCard)\n Total:'
        total, dataframe = self.pvt.pvt_pie1_vol_by_client_other(NotMastercard_df, writer, VOLUME_SHEET, position)
        # pie_vol_by_client_other(self, dataframe, writer, sheet_name, position, total, title,percentage=False)
        self.grh.pie_vol_by_client_other(dataframe, writer, VOLUME_SHEET, position, total, self.title1, values=False,
                                         percentage=True)
        # P2
        position = pvt_position_tuple.pop()
        title2 = 'Ticket Distribution by Shift\n(Exluding MasterCard)\n Total:'
        total, dataframe = self.pvt.pvt_pie2_vol_by_shift_client_other(NotMastercard_df, writer, VOLUME_SHEET, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, VOLUME_SHEET, position, total, title2, percentage=True)
        # P3
        position = pvt_position_tuple.pop()
        title3 = 'Automation Volume\n(Exluding MasterCard)\n Total: '
        total, dataframe = self.pvt.pvt_pie3_vol_by_shift_client_other(NotMastercard_df, writer, VOLUME_SHEET, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, VOLUME_SHEET, position, total, title3, percentage=True)
        # P4
        position = pvt_position_tuple.pop()
        title4 = 'No Automation Volume\n(Exluding MasterCard)\n Total: '
        total, dataframe = self.pvt.pvt_pie4_vol_by_shift_client_other(NotMastercard_df, writer, VOLUME_SHEET, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, VOLUME_SHEET, position, total, title4, percentage=True)

        # Missed SLA
        MISS_SLA_SHEET = 'Miss_SLA'
        pvt_position_tuple = [[12, 26], [0, 26], [12, 5], [0, 5]]
        title1 = 'Missed SLA with Automata\n(Exluding MasterCard)\n Total: '
        title2 = 'Missed SLA Ticket Volume by Shift\n(Exluding MasterCard)\n Total: '
        title3 = 'Missed SLA\n(Exluding MasterCard)\n Total: '
        # P1
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie1_miss_sla_client_other(NotMastercard_df, writer, MISS_SLA_SHEET, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, MISS_SLA_SHEET, position, total, title1, percentage=True)
        # P2
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie2_miss_sla_client_other(NotMastercard_df, writer, MISS_SLA_SHEET, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, MISS_SLA_SHEET, position, total, title2, percentage=True)
        # P3
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie3_miss_sla_client_other(NotMastercard_df, writer, MISS_SLA_SHEET, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, MISS_SLA_SHEET, position, total, title3, percentage=True)

        # MASTERCARD
        # VOL
        VOLUME_SHEET_MC = "Volume_MC"
        pvt_position_tuple = [[12, 36], [0, 36], [12, 5], [0, 5]]
        title1 = 'Ticket Volume by Client\n(Only MasterCard)\n Total:'
        title2 = 'Ticket Distribution by Shift\n(Only MasterCard)\n Total:'  # Distribution
        title3 = 'Automation Volume\n(Only MasterCard)\n Total: '
        title4 = 'Automation Volume\n(Only MasterCard)\n'

        # P1
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie1_vol_by_client_other(Mastercard_df, writer, VOLUME_SHEET_MC, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, VOLUME_SHEET_MC, position, total, title1, percentage=True)
        # P2
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie2_vol_by_shift_client_other(Mastercard_df, writer, VOLUME_SHEET_MC, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, VOLUME_SHEET_MC, position, total, title2, percentage=True)
        # P3
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie3_vol_by_shift_client_other(Mastercard_df, writer, VOLUME_SHEET_MC, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, VOLUME_SHEET_MC, position, total, title3, percentage=True)
        # P4
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie5_vol_by_shift_client_other(Mastercard_df, writer, VOLUME_SHEET_MC, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, VOLUME_SHEET_MC, position, total, title4, values=True)

        # Missed SLA
        MISS_SLA_SHEET_MC = 'Miss_SLA_MC'
        pvt_position_tuple = [[12, 26], [0, 26], [12, 5], [0, 5]]
        title1 = 'Missed SLA with Automata\n(Only MasterCard)\n Total: '
        title2 = 'Missed SLA Ticket Volume by Shift\n(Only MasterCard)\n Total: '
        title3 = 'Missed SLA\n(Only MasterCard)\n Total: '
        title4 = 'Missed SLA\n(Only MasterCard)\n Total: '
        # P1
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie1_miss_sla_client_other(Mastercard_df, writer, MISS_SLA_SHEET_MC, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, MISS_SLA_SHEET_MC, position, total, title1, percentage=True)
        # P2
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie2_miss_sla_client_other(Mastercard_df, writer, MISS_SLA_SHEET_MC, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, MISS_SLA_SHEET_MC, position, total, title2, percentage=True)
        # P3
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie3_miss_sla_client_other(Mastercard_df, writer, MISS_SLA_SHEET_MC, position)
        self.grh.pie_vol_by_client_other(dataframe, writer, MISS_SLA_SHEET_MC, position, total, title3, percentage=True)
        # P4
        position = pvt_position_tuple.pop()
        total, dataframe = self.pvt.pvt_pie4_miss_sla_by_client_ex_mc(Mastercard_df, writer, MISS_SLA_SHEET_MC,
                                                                      position)
        self.grh.pie_vol_by_client_other(dataframe, writer, MISS_SLA_SHEET_MC, position, total, title4, values=True)

    # COLUMN CHART
    def Column_Charts(self, MASTER_WRITER, master_dataframe):
        print('Running Column Charts\n\n')
        # Variable
        writer = MASTER_WRITER
        Mastercard_df = master_dataframe
        # Mastercard_df = master_dataframe[master_dataframe['Client'].str.contains('MasterCard')]
        # NotMastercard_df = master_dataframe[~master_dataframe['Client'].str.contains('MasterCard|AtosUK')]
        sheet_name = 'Mastercard'
        # print(Mastercard_df)
        pvt_position_tuple = [[0, 104], [0, 80], [0, 55], [0, 30], [0, 5]]

        # C1
        position = pvt_position_tuple.pop()
        title = 'SLA Missed by Automata vs Engineer'
        dataframe = self.pvt.pvt_col1_eng_auto_miss_vol(Mastercard_df, writer, sheet_name, position)
        self.grh.cht_eng_auto_miss_vol1(dataframe, writer, sheet_name, position, title)
        # C2
        position = pvt_position_tuple.pop()
        title = 'Volume of Tickets Automata vs Engineer'
        dataframe = self.pvt.pvt_col2_eng_auto_miss_vol(Mastercard_df, writer, sheet_name, position)
        self.grh.cht_eng_auto_miss_vol2(dataframe, writer, sheet_name, position, title)
        # C3
        position = pvt_position_tuple.pop()
        title = 'Volume of Tickets by Shift'
        dataframe = self.pvt.pvt_col3_eng_auto_miss_vol(Mastercard_df, writer, sheet_name, position)
        self.grh.cht_eng_auto_miss_vol3(dataframe, writer, sheet_name, position, title)
        # Resolved

    def RFC_Charts(self, MASTER_WRITER, RFC_REPORT_FILE):
        print('Running RFC\n\n')
        RFC_LOC = '%sRFC/' % self.IN_FOLDER
        sheet_name = 'MasterCard RFC Distribution'
        pvt_position_tuple = [[0, 104], [0, 80], [0, 55], [0, 30], [0, 5]]
        try:
            master_dataframe = self.cdf.DFM_RFC(RFC_LOC, RFC_REPORT_FILE)
            # R1
            title = 'Completed RFC Changes Distribution Over Week'
            position = pvt_position_tuple.pop()
            dataframe = self.pvt.create_pivot_for__RFC(master_dataframe, MASTER_WRITER, sheet_name, position)
            self.grh.RFC_Distribution_Over_Week_Chart(dataframe, MASTER_WRITER, sheet_name, position, title)
            # R2
            title = 'RFC Changes Distribution Over Week\nNot in Completed List'
            position = pvt_position_tuple.pop()
            dataframe = self.pvt.create_pivot_for__RFC_2(master_dataframe, MASTER_WRITER, sheet_name, position)
            self.grh.RFC_Distribution_Over_Week_Chart(dataframe, MASTER_WRITER, sheet_name, position, title)
        except Exception as Exception_Message:
            print("Create DataFrame has failed:\n %s\n" % Exception_Message)
            print("!!!RFC FAILED!!CHECK FOLDER\n")

    def PVT_Charts(self, MASTER_WRITER, master_dataframe, team):
        print('Running PVT Charts\n\n')
        Mastercard_df = master_dataframe[master_dataframe['Client'].str.contains('MasterCard')]
        NotMastercard_df = master_dataframe[~master_dataframe['Client'].str.contains('MasterCard|AtosUK')]
        sheet_name = 'PivotCharts'
        pvt_position_tuple = [[20, 15], [20, 10], [20, 5], [0, 5]]
        position = pvt_position_tuple.pop()
        self.pvt.pvt_eng_resolved(MASTER_WRITER, sheet_name, master_dataframe, position)
        position = pvt_position_tuple.pop()
        title = 'SLA Missed For Other Clients Distribution'
        self.pvt.pvt2_line_client_missed_auto_owned(MASTER_WRITER, sheet_name, NotMastercard_df, position)
        # ------------
        sheet_name = 'ScoreCard'
        input_file = '%s/ScoreCards/%s.xls' % (self.IN_FOLDER, team)
        pvt_position_tuple = [[0, 5], [0, 0]]
        print("SCORE %s" % input_file)
        position = pvt_position_tuple.pop()
        uploaded_sc = pd.read_excel(io=input_file, index=False)
        uploaded_sc = uploaded_sc.fillna('NaN')
        uploaded_sc.to_excel(MASTER_WRITER, sheet_name='%s' % sheet_name, startcol=position[0], startrow=position[1],
                             index=False)
        pivot.pvt_scorecard_conditional(MASTER_WRITER, sheet_name, uploaded_sc, position)
        # cht_eng_auto_miss_vol2(dataframe, MASTER_WRITER, sheet_name, position, title)

    def apps_chart_reports(self, MASTER_WRITER):
        if re.match('.*Apps.*', self.team):
            print('Running APPS Chart Report\n\n')
            # GET FROM DB
            i_sql = "Select * from ScoreCard WHERE Department='Applications Management' and Platform!='CARESTREAM';"
            engine = database_connections('LAB_LOCAL').create_sql_engine()
            master_dataframe = pd.read_sql_query(i_sql, engine)
            # tunnel.close()
            # CREATE REPORT
            SCORE_CARD_SHEET = "AppsScore"
            KPI = 'KPI'
            pvt_position_tuple = [[0, 265], [0, 205], [0, 165], [0, 105], [0, 65], [0, 5]]

            list_series = ['Score', 'MTTRespond(mins)', 'MTTResolve(mins)', 'SLA Misses', 'QoS', 'ASA (Secs)']
            for input_series in list_series:
                # print(master_dataframe['%s' % input_series])
                position = pvt_position_tuple.pop()
                df = self.pvt.pvt_scorecard(MASTER_WRITER, SCORE_CARD_SHEET, master_dataframe, position, input_series)
                self.grh.apps_scorecard(df, MASTER_WRITER, SCORE_CARD_SHEET, position, '%s Timeline' % input_series)

                ##
        if re.match('.*Ent.*', self.team):
            print('Running ENT Chart Report\n\n')
            # GET FROM DB
            i_sql = "Select * from ScoreCard WHERE Department='Enterprise Applications' and Platform!='CARESTREAM';"
            engine = database_connections('LAB_LOCAL').create_sql_engine()
            master_dataframe2 = pd.read_sql_query(i_sql, engine)
            # tunnel.close()
            # CREATE REPORT
            SCORE_CARD_SHEET = "EntScore"
            pvt_position_tuple = [[0, 265], [0, 205], [0, 165], [0, 105], [0, 65], [0, 5]]

            list_series = ['Score', 'MTTRespond(mins)', 'MTTResolve(mins)', 'SLA Misses', 'QoS', 'ASA (Secs)']
            for input_series in list_series:
                # print(master_dataframe['%s' % input_series])
                position = pvt_position_tuple.pop()
                df2 = self.pvt.pvt_scorecard(MASTER_WRITER, SCORE_CARD_SHEET, master_dataframe2, position, input_series)
                self.grh.apps_scorecard(df2, MASTER_WRITER, SCORE_CARD_SHEET, position, '%s Timeline' % input_series)

    def automata_mc_chart(self, MASTER_WRITER):
        sheet_name = 'AutoMata'
        pvt_position_tuple = [[0, 265], [0, 205], [0, 165], [0, 105], [0, 65], [0, 5]]
        position = pvt_position_tuple.pop()
        engine = database_connections('LAB_LOCAL').create_sql_engine()
        #######################################
        sql = 'select * from Weekly_RawMetrics_II where Client=\'MasterCard\''
        mc_dataframe = pd.read_sql_query(sql, engine)
        # pivot
        df = pvt_automata_mc(MASTER_WRITER,sheet_name,position,mc_dataframe)

        # Chart

        self.grh.apps_week_dist(df, MASTER_WRITER, sheet_name, position,
                                'Automata Availability Distribution\nMasterCard')
        # print(df)
        #########################
        # df = pd.pivot_table(dataframe, index='Report_Date', columns=dataframe.ShiftID, values='Client', aggfunc='count')
        # # df = dataframe[['Client', 'ShiftID']].groupby(['ShiftID']).count()
        # # total = df.sum().values[0]
        # # df = df.fillna(value=0)
        # df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position_tuple[0], startrow=position_tuple[1])
        ##########################
    def Test_Charts(self,MASTER_WRITER):
        #POSITION AND NAMES
        sheet_name = 'TEST'
        pvt_position_tuple = [[0, 265], [0, 205], [0, 165], [0, 105], [0, 65], [0, 5]]
        position = pvt_position_tuple.pop()
        engine = database_connections('LAB_LOCAL').create_sql_engine()
        #######################################
        #QUERY
        sql = 'select * from Weekly_RawMetrics_II where Client=\'MasterCard\''
        mc_dataframe = pd.read_sql_query(sql, engine)
        #PIVOT
        dataframe = mc_dataframe[['Report_Date', 'Miss SLA', 'Client']]
        #print(dataframe)
        df = pd.pivot_table(dataframe, index='Report_Date', columns=dataframe['Miss SLA'], values='Client',
                            aggfunc='count')
        print(df)
        # df = df.rename(columns={'Yes': 'Automata Present', 'No': 'Automata Missing'})
        # df = df.fillna(value=0)
        # df.to_excel(writer, sheet_name='%s' % sheet_name, startcol=position[0], startrow=position[1])
        # #CHART
        #
        # self.grh.apps_week_dist(df, MASTER_WRITER, sheet_name, position,
        #                         'Automata Availability Distribution\nMasterCard')
        # print(df)



class update_database(database_connections):
    def __init__(self, team, IN_FOLDER, OUT_FOLDER):
        database_connections.__init__(self, "LAB_LOCAL")
        self.engine = self.create_sql_engine()
        self.team = team
        self.IN_FOLDER = IN_FOLDER
        self.OUT_FOLDER = OUT_FOLDER

    def update_megalog(self, report_date, team):
        if team == 'Ent':
            regteam = "%Enterprise Applications%"
        if team == 'Apps':
            regteam = "%Applications Management%"
        list_of_dates = pd.read_sql_query(
            "select distinct(Report_Date) from Weekly_RawMetrics_II where `Initial Department` like \'%s\';" % regteam,
            self.engine)
        if report_date not in list(list_of_dates['Report_Date']):
            print("UPLOADING MEGA LOG FOR %s" % self.team.upper())
            master_dataframe = create_megalog(self.team, self.IN_FOLDER, self.OUT_FOLDER).read_data()
            y = pd.Series(name='Report_Date')
            appd_rd = pd.concat([y, master_dataframe], axis=1)
            appd_rd = appd_rd.fillna(value={'Report_Date': report_date})
            print(appd_rd.head())
            appd_rd.to_sql('Weekly_RawMetrics_II', self.engine, index=False, if_exists='append')
        else:
            print("Data Already In DataBase")

    def upload_ScoreCard(self, report_date, team):
        if team == 'Ent':
            regteam = "%Enterprise Applications%"
        if team == 'Apps':
            regteam = "%Applications Management%"
        list_of_dates = pd.read_sql_query(
            'select distinct(Date) from ScoreCard where Department like \'{}\';'.format(regteam), self.engine)
        print(list(list_of_dates['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))))
        if report_date not in list(list_of_dates['Date'].apply(lambda x: x.strftime('%Y-%m-%d'))):
            print("UPLOADING SCORECARD FOR %s!!" % self.team.upper())
            uploaded_sc = pd.read_excel(io='%s/ScoreCards/%s.xls' % (self.IN_FOLDER, team))
            y = pd.Series(name='Date')
            appd_rd = pd.concat([y, uploaded_sc], axis=1)
            appd_rd = appd_rd.fillna(value={'Date': report_date})
            appd_rd = appd_rd.rename(
                columns={'MTTRespond (Min)': 'MTTResolve(mins)', 'MTTResolve (Min)': 'MTTResolve(mins)',
                         'ASA (Sec)': 'ASA (Secs)', 'AR%': 'AR (%)'})
            print(appd_rd.head())
            appd_rd.to_sql('ScoreCard', self.engine, index=False, if_exists='append')
        else:
            print("Data Already In DataBase")


class functions:
    def test(self):
        import datetime, os, time
        file = '/home/amcdowald/test.sh'
        file_modify_date = datetime.datetime.strptime(time.ctime(os.path.getctime(file)), "%a %b %d %H:%M:%S %Y").date()
        system_date = datetime.datetime.now().date()
        if file_modify_date == system_date:
            print('yes')
        else:
            print('no')

    @staticmethod
    def getfiles(mypath):
        onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
        names = []
        for i_file in onlyfiles:
            names.append('{path}{i_file}'.format(path=mypath, i_file=i_file))
        return names



if __name__ == "__main__":
    enterprise_reporting = reporting('Ent')
    application_reporting = reporting('Apps')
    application_reporting.OUT_FOLDER = '/home/amcdowald/OPs_Metrics/out/'
    application_reporting.IN_FOLDER = '/home/amcdowald/OPs_Metrics/in/'
    enterprise_reporting.OUT_FOLDER = '/home/amcdowald/OPs_Metrics/out/'
    enterprise_reporting.IN_FOLDER = '/home/amcdowald/OPs_Metrics/in/'
    application_reporting.RFC_REPORT_FILE = 'MC RFC Report-Jan7-Jan13-2018'
    application_reporting.attendance = 'Shared Apps Team  Attendance - Jan 7 to Jan 13 - 2018'
    # functions().test()

    # application_reporting.testFunc()
    # create_megalog('Apps',application_reporting.IN_FOLDER,application_reporting.OUT_FOLDER).create_tlog()
    # e=database_connections('LAB_LOCAL').create_sql_engine()
    # print(pd.read_sql_query('show tables;',e))


    update = False
    if update:
        report_date = '2018-01-14'
        print("UPLOADING APPS")
        update_database(application_reporting.team, application_reporting.IN_FOLDER,
                        application_reporting.OUT_FOLDER).update_megalog(report_date, 'Apps')
        update_database(application_reporting.team, application_reporting.IN_FOLDER,
                        application_reporting.OUT_FOLDER).upload_ScoreCard(report_date, 'Apps')
        print("UPLOADING ENT")
        update_database(enterprise_reporting.team, enterprise_reporting.IN_FOLDER,
                        enterprise_reporting.OUT_FOLDER).update_megalog(report_date, 'Ent')
        update_database(enterprise_reporting.team, enterprise_reporting.IN_FOLDER,
                        enterprise_reporting.OUT_FOLDER).upload_ScoreCard(report_date, 'Ent')

    print('STARTING')
    create_megalog('Apps', application_reporting.IN_FOLDER, application_reporting.OUT_FOLDER).create_tlog()
    application_reporting.run_reporting()
    print('STARTING')
    enterprise_reporting.OUT_FOLDER = '/home/amcdowald/OPs_Metrics/out/'
    enterprise_reporting.IN_FOLDER = '/home/amcdowald/OPs_Metrics/in/'
    create_megalog('Ent', enterprise_reporting.IN_FOLDER, enterprise_reporting.OUT_FOLDER).create_tlog()
    enterprise_reporting.run_reporting()