#!/usr/bin/python2.7
import csv

#import re
#import pymysql
#import xlrd
#import xlwt
#import re
#import time
#import sys, getopt
#from sql_calls import sql_calls
#from Transform_ER import transf_er, find_matching_rows, find_header_index, write_line, xlxs_func, getfiles, match_and_replace2
#import re
import datetime
#from os import listdir
#from os.path import isfile, join

from transform_data import *

def if_replace(i_file, i_sheetname, columName,match_r, replace,o_file=None):
  """ input, search,replace, write, and output header and rows array.
  i_file = "/home/amcdowald/wrksite/Metrics/outputs/text.xls"
  i_sheetname = "Sheet1"
  columName = "Day"
  match_r = 'Wed.*'
  replace = 'here'
  o_file = "/home/amcdowald/wrksite/Metrics/outputs/text_o.xls"
  """
  rD_book, rD_sheet, wT_book, wT_sheet =xlxs_func(i_sheetname,i_file)
  header_index, header_line = find_header_index(rD_sheet, "%s" % columName)
  row_list  = map(lambda row: rD_sheet.row_slice(rowx=row, start_colx=0,end_colx=rD_sheet.ncols), [row for row in range(1,rD_sheet.nrows)])
  for row in range(0,rD_sheet.nrows-1):
    # ##### input = {colum_value, match_r, replace}
    if re.match("%s" % match_r,row_list[row][header_index].value):
      row_list[row][header_index].value= '%s' % replace
      print("Match: replaced \"%s\" with \"%s\"" % (match_r,replace) )
    #if (int(match_r) > colum_value):
      #colum_value = function()
    # #####
  if (o_file):  
    write_header(rD_sheet,wT_sheet,"%s" % columName )
    write_line(1,wT_sheet,row_list)
    wT_book.save(o_file)
  return header_line ,row_list
def function():
  return 1

def find_column(sheet,colName):
  row_s = sheet.row_slice(rowx=0, start_colx=0,end_colx=sheet.ncols)
  print(colName)
  index = [index for index, list1 in enumerate(row_s) if (list1.value == '%s' % colName)]
  #print(row_s[index[0]],colName)
  return index[0]

i_file = "/home/amcdowald/wrksite/Metrics/inputs/text_o.xls"
i_sheetname = "Sheet1"
columName = "Day"
match_r = 'here'
replace = 'Wednesday'
o_file = "/home/amcdowald/wrksite/Metrics/outputs/text_o2.xls"

i_file="/home/amcdowald/wrksite/Metrics/outputs/Tlog_Total.xls"
i_sheetname="TLog"
#x,y = if_replace(i_file, i_sheetname, columName,match_r, replace,o_file)
rD_book, rD_sheet, wT_book, wT_sheet =xlxs_func(i_sheetname,i_file)
num, line = find_header_index(rD_sheet, "Miss SLA")

rows = find_matching_rows(rD_sheet,num,"Yes",rD_sheet.nrows)
print(len(rows))
for row in rows:
  l = rD_sheet.row_slice(rowx=row, start_colx=0,end_colx=1) #rD_sheet.col_values(0,start_rowx=row,end_rowx=rD_sheet.nrows)
print(l)
x=set([x for x in l if l.count(x) >= 1])
print(x)
#print([line for line in y])
#def replace_val_in_row(match,col_index,row_list,replace):
  #"""match = re.compile(r'%s' % str(regex2))
     #col_index = header_index[0]
     #replace = "hello"""
  ##index = col_index[0]
  #rows = []
  #for index in col_index:
    #for value in row_list:
      #if match.match(value[index].value):
	#value[index].value = replace
      #rows.append(value)
  #return rows

#def main2():
  #i_sql = """select l.Username, cs.StartDate, cs.EndDate,cs.CallSessionID, (select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = 638827) as CallSession_Queued_Date from IPcc.CallSession cs left join auth.CLIENT c on cs.ClientID = c.clientID left join IPcc.CallHotline ch on cs.CallHotlineID = ch.CallHotlineID left join auth.LOGIN l on cs.LoginID = l.loginID left  join auth.INTERNAL_LOGIN ail on (ail.LoginID = cs.LoginID) left join auth.DEPARTMENT ad on (ad.DepartmentID = ail.DepartmentID) left join auth.OFFICE ao on (ail.OfficeID = ao.OfficeID) where cs.StartDate between date_sub((select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = 638827),interval 30 minute) and (select QueuedDate from IPcc.CallSession cs where cs.CallSessionID = 638827) and ad.Name like  'Applications Management' """
  #sql_calls(5,i_sql=i_sql,file_o='/home/amcdowald/wrksite/Metrics/outputs/Shared_Tlogs.csv')
  #print("here with me")

##parser = argparse.ArgumentParser(description='Process some integers.')
##parser.add_argument('integers', metavar='N', type=int, nargs='+',
                    ##help='an integer for the accumulator')
##parser.add_argument('--sum', dest='accumulate', action='store_const',
                    ##const=sum, default=max,
                    ##help='sum the integers (default: find the max)')

##args = parser.parse_args()
##print args.accumulate(args.integers)

###import csv
###import re
###import pymysql
#import xlrd
#import xlwt
#import re
#import time
#import sys, getopt
#from sql_calls import sql_calls
#from Transform_ER import transf_er, find_matching_rows, find_header_index, write_line, xlxs_func
#import re

##def 
  ##i_book = xlrd.open_workbook("%s" % i_file)
  ##i_sheet = i_book.sheet_by_name("%s" % i_sheetname)
  ##end_row=i_sheet.nrows
  ###Out
  ##o_book = xlwt.Workbook()
  ##o_sheet = o_book.add_sheet('t_%s' % i_sheetname )

#start_row=1
#i_sheetname="TLog"
#regex_column='Initial Department'
#regex_col_value='Applications Management'
#row_array = []
#file_list = ["/home/amcdowald/wrksite/Metrics/inputs/RawMetrics/paas_ops_metrics.xls", "/home/amcdowald/wrksite/Metrics/inputs/RawMetrics/vnsny_ops_metrics.xls"]
#for i_file in file_list:
  #rD_book, rD_sheet, wT_book, wT_sheet =xlxs_func(sheetname=i_sheetname,input_d=i_file)
  ##this finds the header for
  #header_index, header_line = find_header_index(rD_sheet, regex_column)
  #end_row=rD_sheet.nrows
  #match = re.compile(r'%s' % str(regex_col_value))
  #matched_rows = find_matching_rows(rD_sheet,header_index, regex_col_value, end_row)
  #row_list = map(lambda row: rD_sheet.row_slice(rowx=row, start_colx=0,end_colx=rD_sheet.ncols), matched_rows)
  #for line in row_list:
    #row_array.append(line)
  #row_list=row_array
#for index, col in enumerate(header_line):
  #wT_sheet.write(0,index, col.value)
#write_line(start_row,wT_sheet,row_list)
#o_file = "/home/amcdowald/wrksite/Metrics/outputs/{title}-{date}.xls".format(title=i_sheetname, date=time.asctime( time.localtime(time.time()) ))
#wT_book.save(o_file)


#for index,value in enumerate(data):
  #sheet2.write(index,0,value)
  
#row_s = sheet.row_slice(sheet.col_values,start_rowx=1,end_rowx=sheet.nrows)
#list1 = [index for index, list1 in enumerate(row_s) if (list1.value == '%s' % colName)]
#for row in range(sheet.nrows):
  #for line in sheet.col_values(col,start_rowx=1,end_rowx=sheet.nrows):
#tip=find_column(sheet,'Ticket Type')  
  #print(sheet.row(row))
#for col in range(0,sheet.ncols):

 #for col in range(sheet.ncols):
    #print(col)
    
#for row in range
#row = sheet.row_slice(rowx=row, start_colx=0,end_colx=sheet.ncols)]
#print(row)
#def wT_column(sheet,columnNum,array):
  #for index,value in enumerate(array):
    #sheet.write(index,columnNum,value)#time
    

##row_s = sheet.row_slice(rowx=row, start_colx=0,end_colx=sheet.ncols)
##list1  = [list1 for index, list1 in enumerate(row_s)]# if (list1.value == '%s' % colName)]
##x=find_column(sheet,'Ticket Type')

#def find_header_index(i_sheet, regex):
  #try:
    #ticket = re.compile(r'%s' % str(regex))
    #header = i_sheet.row_slice(rowx=0, start_colx=0,end_colx=i_sheet.ncols)
    #col_index = [index for index, title in enumerate(header) if ticket.match(title.value)]
  #except NameError:
    #print("Header error")
  #return col_index, header


#def find_matching_rows(sheet,headerIndex,regex,end_row):
  #match = re.compile(r'%s' % str(regex))
  #rows = [rows for rows in range(end_row) if (match.match(sheet.cell(rows,headerIndex[0]).value))]
  #return rows

#def replace_val_in_row(match,col_index,row_list,replace):
  #"""match = re.compile(r'%s' % str(regex2))
     #col_index = header_index[0]
     #replace = "hello"""
  ##index = col_index[0]
  #rows = []
  #for index in col_index:
    #for value in row_list:
      #if match.match(value[index].value):
	#value[index].value = replace
      #rows.append(value)
  #return rows

#def write_line(start_row,o_sheet,row_line):
  #for index, line in enumerate(row_line):
    #for index2, col in enumerate(line):
      #o_sheet.write(index+start_row,index2,col.value)
      
##
##=
##regex_column = 
##regex_col_value = 
##replace = "helloHE:ASA"



#def main(i_match,i_sheetname, i_book,regex_column,regex_col_value,replace='None'):
  #start_row=1
  ##In
  #i_book = xlrd.open_workbook("%s" % i_book)
  #i_sheet = i_book.sheet_by_name("%s" % i_sheetname)
  #end_row=i_sheet.nrows
  ##Out
  #o_book = xlwt.Workbook()
  #o_sheet = o_book.add_sheet('t_%s' % i_sheetname )
  
  #match = re.compile(r'%s' % str(regex_col_value))
  #header_index, header_line = find_header_index(i_sheet, regex_column) #this finds the header for
   ##write header
  #for index, col in enumerate(header_line):
    #o_sheet.write(0,index, col.value) 
  #if re.match(r'.*',str(regex_col_value)):
    #matched_rows = find_matching_rows(i_sheet,header_index, regex_col_value, end_row)
    #row_list = map(lambda row: i_sheet.row_slice(rowx=row, start_colx=0,end_colx=i_sheet.ncols), matched_rows)
    #row_line = row_list
  #if (not re.match(r'None',str(replace))):
    #matched_rows = find_matching_rows(i_sheet,header_index, regex_col_value, end_row)
    #row_list = map(lambda row: i_sheet.row_slice(rowx=row, start_colx=0,end_colx=i_sheet.ncols), matched_rows)
    #row_line = replace_val_in_row(match,header_index,row_list,replace)
  #write_line(start_row,o_sheet,row_line)
  #o_book.save("/home/amcdowald/wrksite/Metrics/outputs/{title}-{date}.xls".format(title=i_sheetname, date=time.asctime( time.localtime(time.time()) )    ))

#main(i_match="Y",i_sheetname="TLog",i_book="/home/amcdowald/wrksite/Metrics/Applications_Management_2017-08-15.xlsx",regex_column='Ticket Type',regex_col_value='CHNG')#,replace="bingo")


#for value in row_list:
    #if match.match(value[header_index[0]].value):
      #value[header_index[0]] = replace
    #print(value)
#for row in rows: sheet.row_slice(rowx=row, start_colx=0,end_colx=sheet.ncols)
  #print(line[header_index[0]])
  
  
#print(index)
#print(header[index].value)
#print(sheet.cell(2,2))
#if (str(list1[x].value) == "IPMON"):
  #list1[x] = "Yeah"
#print(list1)
#columnNum = find_column(sheet,'Ticket Type')
#for col in range(sheet.ncols):
  #sheet.row_slice(rowx=row, start_colx=0,end_colx=sheet.ncols)]
  #column = sheet.col_values(col,start_rowx=start_row,end_rowx=end_row)   
  #print(column)
    
    #x = sheet.col_values(col,start_rowx=0,end_rowx=2)
    #print(x)
    #print('\n')
#for line in sheet.col_values(1,start_rowx=1,end_rowx=4):
  #if (line == '%s' % 'IPMON'):
    #print(line)

#for col in range(0,sheet.ncols):
  #for line in sheet.col_values(col,start_rowx=1,end_rowx=sheet.nrows):
    #tip=find_column(sheet,'Ticket Type')
    #print(tip)
    #if (list1.value == '%s' % 'Ticket Type'):
      #print(line)
    
#for index, list1 in enumerate(row_s):
  #if (list1.value == '%s' % 'Ticket Type'):
    #print(row_s)

#for row in sheet.col_values(17,start_rowx=1,end_rowx=sheet.nrows):
  
#import xlrd
#import xlwt
#import re

#choice = 1
#if re.match(r"(0|1|2)", str(choice)):
  #print("MATCH")
#else:
  #print("NO MATCH")

#print("Time Stripper: I will take the \" \" ")
#book = xlrd.open_workbook("/home/amcdowald/wrksite/Metrics/Applications_Management_2017-08-15.xlsx") #open our xls file, there's lots of extra default options in this call, for logging etc. take a look at the docs
 
##sheet = book.sheets()[0] #book.sheets() returns a list of sheet objects... alternatively...
#sheet = book.sheet_by_name("TLog") #we can pull by name
 
#r = sheet.row(0) #returns all the CELLS of row 0,
#c = sheet.col_values(0) #returns all the VALUES of row 0,
##print(sheet.nrows)
##print(sheet.ncols)
##print(sheet.col_values(17,start_rowx=0,end_rowx=1).pop())
###obj = re.compile(r'(2017)-(\d+)-(\d+)(.*)', re.IGNORECASE)
 ##for col in [num for num in range(1,rD_sheet.ncols) if num !=columnNum]:
#row_s = sheet.row_slice(rowx=0, start_colx=0,end_colx=sheet.ncols)
#index = [index for index, list1 in enumerate(row_s) if (list1.value == 'Ticket Created (UTC)')]
    
#print(index[0])


#for col in r:
  #print(type(col))
  #line = re.sub(r'^text:(.*)', r'\g<1>', col)
  #print(line)
#data = []
#data.append('Date')
#for row in sheet.col_values(17,start_rowx=1,end_rowx=sheet.nrows):
  #line = re.sub(r'(2017)-(\d+)-(\d+)(.*)', r'\g<2>-\g<3>-\g<1>', row)#"%s-%s-%s" % (re.group(2),re.group(3),re.group(1)), row)
  #data.append(line) #data = [] #make a data store
  #print(line)
#workbook2 = xlwt.Workbook("/home/amcdowald/wrksite/Metrics/outputs/App.xlsx")
#sheet2 = workbook2.add_sheet('test')

#for index,value in enumerate(data):
  #sheet2.write(index,0,value)
  
#workbook2.save("/home/amcdowald/wrksite/Metrics/outputs/App.xlsx")

  #new
#mod=17
###workbook2.save("text.xlsx")
#data=[]
#for col in range(sheet.ncols):
  #data.append(sheet.col_values(col).pop())
  #for row in sheet.col_values(col,start_rowx=1,end_rowx=sheet.nrows):
    #if col == mod:
      #line = re.sub(r'(2017)-(\d+)-(\d+)(.*)', r'\g<2>-\g<3>-\g<1>', row)#"%s-%s-%s" % (re.group(2),re.group(3),re.group(1)), row)
      #data.append(line) #data = [] #make a data store
      #for index,value in enumerate(data):
          #sheet2.write(index,col,value)
    #else:
      #data.append(row)
      #data=[]



#spamReader = csv.DictReader(open("/home/amcdowald/wrksite/Metrics/Data.csv"), delimiter=',', quotechar='"')
#contacted = []
#available = []
#called = []
#ignored_unallocated = []
#busy_on_phone = []
##get data from  Data
#for row in spamReader:
  #lines=row["Data"]
  ##CONTACTED ENGINEERS
  #contacted_engineers=re.compile(r'.*Calling engineer\(s\):(.*)\s(\(.*\))')
  #if contacted_engineers.match(lines):
    #output = contacted_engineers.sub(r'\g<1>',lines)
    #called.append(output)
    ##print(output)
    ##print(line for line in output)
  #output3=[]
  ##engineers available
  #available_engineers=re.compile(r'E.*-\s{(.*)}.*Busy/C.*')
  #if available_engineers.match(lines):
    #output2=available_engineers.sub(r'\g<1>',lines)
    #output2=re.findall(r'[A-z]+\s[A-z]+',output2)
    #output2.sort();
    #available.append(output2)
    ##print(Busy/On Phone:\n",output2)
  ##busy on the phone  
  #available_engineers=re.compile(r'.*Busy/O.*{([A-z]+\s[A-z]+.*)}\W<hr />Ignored.*')
  #if available_engineers.match(lines):
    #output2=available_engineers.sub(r'\g<1>',lines)
    #output2=re.findall(r'[A-z]+\s[A-z]+',output2)
    #output2.sort();
    #busy_on_phone.append(output2)

    ##print(output2)
    
  #available_engineers=re.compile(r'.*<hr />Ignored UNALLOCATED numbers:.*]\W(.*).*<hr />C.*')
  #if available_engineers.match(lines):
    #output2=available_engineers.sub(r'\g<1>',lines)
    #output2=re.findall(r'[A-z]+\s[A-z]+',output2)
    #output2.sort();
    #ignored_unallocated.append(output2)
    ##print("Ignored UNALLOCATED numbers:\n",output2)
  #available_engineers=re.compile(r'.*<hr />Currently contacted.*]\W{(.*)}.*')
  #if available_engineers.match(lines):
    #output2=available_engineers.sub(r'\g<1>',lines)
    #output2=re.findall(r'[A-z]+\s[A-z]+',output2)
    #output2.sort();
    #contacted.append(output2)
    

#output = open('/home/amcdowald/wrksite/Metrics/Data_On_Dispatch.csv', 'w')
#fnames = ['Engineers_Called', 'Available_Engineers', 'Busy_On_Phone','Ignored_unallocated']
#writer = csv.DictWriter(output, fieldnames=fnames) 
#writer.writeheader()
#for col in fnames:
  #writer.writerow({
  #'Engineers_Called' : '%s' % (called.pop() if len(called) != 0 else None ),
  #'Available_Engineers' : '%s' % (available.pop() if len(available) != 0 else None ),
  #'Busy_On_Phone' : '%s' % (busy_on_phone.pop() if len(busy_on_phone) != 0 else None), 
  #'Ignored_unallocated' : '%s' % (ignored_unallocated.pop() if len(ignored_unallocated) != 0 else None)
  #})
  
  
#'Engineers available: [6] - {Erison Melo (emelo) (6986),Sairam Leelahar (smleelah) (6106),Sahana Ghosh (saghosh) (6182),Olliec Crenshaw (ocrenshaw) (6956),Emmanuel Ojo (eojo) (6936),Anish Kesavankutty (akesavan) (6007)}
#<hr />Busy/Checked_In on IPcenter: [0] {} 
#<hr />Busy/On Phone (CUPS) [4] {Mutyalaiah Arcot (marcot) (6379),Sabareesan Nagarajan (sanagaraj) (6453),Vinay Narayanaswamy (vnarayan) (6189),Siva Juturu (sjuturu) (6174)} 
#<hr />Ignored UNALLOCATED numbers: [2] Sandeep Talakkilevalappil (stalakil) (5848),Anthony Castillo (acastillo) (5639) <hr />Currently contacted [0] {} <hr />'







#def diff(first, second):
  #second = set(second)
  #return [item for item in first if item not in second]

#spamReader = csv.DictReader(open("/home/amcdowald/Documents/test_asa.csv"), delimiter=',', quotechar='"')
##get data from  Data
#for row in spamReader:
  #lines=row["Data"]
  ##CONTACTED ENGINEERS
  #contacted_engineers=re.compile(r'.*Calling engineer\(s\):(.*)\s(\(.*\))')
  #if contacted_engineers.match(lines):
    #output = contacted_engineers.sub(r'\g<1>',lines)
    ##print(output)
  #output3=[]
  ##engineers available
  #available_engineers=re.compile(r'E.*-\s{(.*)}.*Busy/C.*')
  #if available_engineers.match(lines):
    #output2=available_engineers.sub(r'\g<1>',lines)
    #output2=re.findall(r'[A-z]+\s[A-z]+',output2)
    #output2.sort();
    ##print(Busy/On Phone:\n",output2)
  ##busy on the phone  
  #available_engineers=re.compile(r'.*Busy/O.*{([A-z]+\s[A-z]+.*)}\W<hr />Ignored.*')
  #if available_engineers.match(lines):
    #output2=available_engineers.sub(r'\g<1>',lines)
    #output2=re.findall(r'[A-z]+\s[A-z]+',output2)
    #output2.sort();
    ##print(output2)
    
  #available_engineers=re.compile(r'.*<hr />Ignored UNALLOCATED numbers:.*]\W(.*).*<hr />C.*')
  #if available_engineers.match(lines):
    #output2=available_engineers.sub(r'\g<1>',lines)
    #output2=re.findall(r'[A-z]+\s[A-z]+',output2)
    #output2.sort();
    #print("Ignored UNALLOCATED numbers:\n",output2)
  #available_engineers=re.compile(r'.*<hr />Currently contacted.*]\W{(.*)}.*')
  #if available_engineers.match(lines):
    #output2=available_engineers.sub(r'\g<1>',lines)
    #output2=re.findall(r'[A-z]+\s[A-z]+',output2)
    #output2.sort();
    #print("Engineers contacted:\n",output2) 
    
    #Engineers available: [6] - {Erison Melo (emelo) (6986),Sairam Leelahar (smleelah) (6106),Sahana Ghosh (saghosh) (6182),Olliec Crenshaw (ocrenshaw) (6956),Emmanuel Ojo (eojo) (6936),Anish Kesavankutty (akesavan) (6007)} <hr />Busy/Checked_In on IPcenter: [0] {} <hr />Busy/On Phone (CUPS) [4] {Mutyalaiah Arcot (marcot) (6379),Sabareesan Nagarajan (sanagaraj) (6453),Vinay Narayanaswamy (vnarayan) (6189),Siva Juturu (sjuturu) (6174)}
    #<hr />Ignored UNALLOCATED numbers: [2] Sandeep Talakkilevalappil (stalakil) (5848),Anthony Castillo (acastillo) (5639) <hr />Currently contacted [0] {} <hr />
#regex = re.compile('([a-zA-Z]\"[a-zA-Z])', re.S)
#myfile =  'foo"s bar'
#myfile2 = regex.sub(lambda m: m.group().replace('"',"%",1), myfile)

#'Engineers available: [6] - {Erison Melo (emelo) (6986),Sairam Leelahar (smleelah) (6106),Sahana Ghosh (saghosh) (6182),Olliec Crenshaw (ocrenshaw) (6956),Emmanuel Ojo (eojo) (6936),Anish Kesavankutty (akesavan) (6007)}
#<hr />Busy/Checked_In on IPcenter: [0] {} 
#<hr />Busy/On Phone (CUPS) [4] {Mutyalaiah Arcot (marcot) (6379),Sabareesan Nagarajan (sanagaraj) (6453),Vinay Narayanaswamy (vnarayan) (6189),Siva Juturu (sjuturu) (6174)} 
#<hr />Ignored UNALLOCATED numbers: [2] Sandeep Talakkilevalappil (stalakil) (5848),Anthony Castillo (acastillo) (5639) <hr />Currently contacted [0] {} <hr />'




#mysql --user=readonly --password=read0nly -h ipdb-s --skip-column-names
#pymysql.connect(host='localhost', port=3306, user='root', passwd='', db='mysql')
#connection = pymysql.connect(host='localhost',
			     #port=12345,
                             #user='readonly',
                             #password='read0nly',
			     #db='*'
			   #)


#try:
    #with connection.cursor() as cursor:
        ## Read a single record
        #sql ="""
	  #select cs.CallSessionID, lg.Username ,cl.Data,cl.EventDate, cs.ClientID, cs.CallerPhoneNumber, cs.LoginID, cs.QueuedDate, cs.Status
	  #from IPcc.CallSession cs 
	  #inner join auth.INTERNAL_LOGIN il on cs.LoginID = il.LoginID
	  #inner join auth.LOGIN lg on (lg.LoginID = il.LoginID)
	  #inner join auth.LOGIN_AVAILABILITY la on il.LoginAvailabilityID = la.LoginAvailabilityID
	  #inner join auth.DEPARTMENT ad on (il.DepartmentID = ad.DepartmentID)
	  #inner join auth.SHIFT sh on (il.ShiftID = sh.ShiftID)
	  #inner join IPcc.CallLog cl on (cl.CallSessionID=cs.CallSessionID)
	  #inner join auth.LOGIN_AVAILABILITY_HISTORY lh
	  #where cs.CallSessionID = 638827 and (cl.Data like '%Calling%' or cl.Data like'Engineers%') group by cl.Data
	  #"""
        
        #cursor.execute(sql)
        
        ##cursor.execute(sql, ('638827',))
        #result = cursor.fetchone()
        #print(result)
#finally:
    #connection.close()






















#(\([a-z]+\)\W+\([\d]+\),)
#import xlrd
#import xlwt
#import re

#book = xlrd.open_workbook("/home/amcdowald/Documents/App.xlsx") #open our xls file, there's lots of extra default options in this call, for logging etc. take a look at the docs
 
#sheet = book.sheets()[0] #book.sheets() returns a list of sheet objects... alternatively...
#sheet = book.sheet_by_name("TLog") #we can pull by name
 
#r = sheet.row(0) #returns all the CELLS of row 0,
#c = sheet.col_values(0) #returns all the VALUES of row 0,
##print(sheet.nrows)
##print(sheet.ncols)
##print(sheet.col_values(17,start_rowx=0,end_rowx=1).pop())
###obj = re.compile(r'(2017)-(\d+)-(\d+)(.*)', re.IGNORECASE)

#data = []
#data.append('Date')
#for row in sheet.col_values(17,start_rowx=1,end_rowx=sheet.nrows):
  #line = re.sub(r'(2017)-(\d+)-(\d+)(.*)', r'\g<2>-\g<3>-\g<1>', row)#"%s-%s-%s" % (re.group(2),re.group(3),re.group(1)), row)
  #data.append(line) #data = [] #make a data store

#workbook2 = xlwt.Workbook("/home/amcdowald/Documents/App.xlsx")
#sheet2 = workbook2.add_sheet('test')

#for index,value in enumerate(data):
  #sheet2.write(index,0,value)
  
  ##new
##mod=17
####workbook2.save("text.xlsx")
##data=[]
##for col in range(sheet.ncols):
  ##data.append(sheet.col_values(col).pop())
  ##for row in sheet.col_values(col,start_rowx=1,end_rowx=sheet.nrows):
    ##if col == mod:
      ##line = re.sub(r'(2017)-(\d+)-(\d+)(.*)', r'\g<2>-\g<3>-\g<1>', row)#"%s-%s-%s" % (re.group(2),re.group(3),re.group(1)), row)
      ##data.append(line) #data = [] #make a data store
      ##for index,value in enumerate(data):
          ##sheet2.write(index,col,value)
    ##else:
      ##data.append(row)
      ##data=[]
#workbook2.save("text.xlsx")