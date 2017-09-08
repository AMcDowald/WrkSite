import csv
import re
import pymysql
import subprocess
import sys, getopt

CallSessionID = int(sys.argv[1])# 641167
choice = int(sys.argv[2])
print(CallSessionID)

def write_from_sql(file_i):
  with open(file_i, 'w') as fout:
    print(fout)
    writer = csv.writer(fout)
    writer.writerow([ i[0] for i in cursor.description ]) # heading row
    writer.writerows(cursor.fetchall())

def writeSt(file_o,path2script=0,args=0):
  with open(file_o, 'w') as fout:
    print(file_o)
    writer = csv.writer(fout)
    writer.writerow([ i[0] for i in cursor.description ]) # heading row
    writer.writerows(cursor.fetchall())
  command = 'Rscript'
  print(args)
  cmd = [command, path2script] + args
  out = subprocess.check_output(cmd, universal_newlines=True)
  print(out)
  
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




connection = pymysql.connect(host='localhost',
			     port=12345,
                             user='readonly',
                             password='read0nly',
			     db='IPradar'
			   )


try:
  with connection.cursor() as cursor:
    if (choice == 0):
      print("Getting Status Changes Of All Engineers\n 30 mins before CallSessionID: %s\n Was Bridged" % CallSessionID)
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
      group by lh.Created'''.format(varb=CallSessionID) 
      cursor.execute(sql)
      file_o = "/home/amcdowald/wrksite/Metrics/outputs/Status_Change_%s.csv" % (CallSessionID)
      path2script = '/home/amcdowald/wrksite/Metrics/asa_call_data2.R'
      args = ['/home/amcdowald/wrksite/Metrics/outputs/Status_Change_%s.csv' % (CallSessionID),'%s' % (CallSessionID)]
      writeSt(file_o,path2script,args)
    elif (choice == 1):
      print("Getting All Calls to Engineers\n 30 mins before CallSessionID: %s\n Was Bridged" % CallSessionID)
      sql = "select l.Username, cs.StartDate, cs.EndDate,cs.CallSessionID, (select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = %d) as CallSession_Queued_Date from IPcc.CallSession cs left join auth.CLIENT c on cs.ClientID = c.clientID left join IPcc.CallHotline ch on cs.CallHotlineID = ch.CallHotlineID left join auth.LOGIN l on cs.LoginID = l.loginID left  join auth.INTERNAL_LOGIN ail on (ail.LoginID = cs.LoginID) left join auth.DEPARTMENT ad on (ad.DepartmentID = ail.DepartmentID) left join auth.OFFICE ao on (ail.OfficeID = ao.OfficeID) where cs.StartDate between date_sub((select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = %d),interval 30 minute) and (select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = %d) and ad.Name like  'Applications Management' " % (CallSessionID,CallSessionID,CallSessionID)
      cursor.execute(sql)
      file_o = '/home/amcdowald/wrksite/Metrics/outputs/Calls_During_%s.csv' % (CallSessionID)
      path2script = '/home/amcdowald/wrksite/Metrics/asa_call_data.R'
      args = ['/home/amcdowald/wrksite/Metrics/outputs/Calls_During_%s.csv' % (CallSessionID),'%s' % (CallSessionID) ]
      writeSt(file_o,path2script,args)
    elif (choice == 2):
      print("Getting All IPcc history for CallSessionID: %s\n" % CallSessionID)
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
      inner join IPcc.CallLog cl on (cl.CallSessionID=cs.CallSessionID)""" + "where cs.CallSessionID like '%d'" % (CallSessionID) + "and lg.Username like '%' and ad.Name like 'Applications%' and (cl.Data like '%Calling%' or cl.Data like'Engineers%') group by cl.Data"
      cursor.execute(sql)
      file_i = '/home/amcdowald/wrksite/Metrics/outputs/Data_%s.csv' % (CallSessionID)
      file_o = '/home/amcdowald/wrksite/Metrics/outputs/Log_History_CallID:%s.csv' % (CallSessionID)
      write_from_sql(file_i)
      writeSt2(file_i,file_o)
finally:
  connection.close()