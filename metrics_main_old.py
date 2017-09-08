#!/usr/bin/python2.7
import sys, getopt
from sql_calls import sql_calls
from Transform_ER import transf_er2, find_matching_rows, find_header_index, write_line, xlxs_func, getfiles, match_and_substitute, find_column
import re
import time
import numpy
# 641167

warnings = """
	   Options:
	   
	   0 - Getting All Calls to Engineers 30 mins before CallSessionID:  Was Bridged"
	   1 - Getting All Calls to Engineers 30 mins before CallSessionID: Was Bridged 
	   2 - Getting All IPcc history for CallSessionID
	   3 - This separates the UTC Datetime to Date and Time
	   4 - This grabs the TLog of Shared from DB and place in CSV
	   """
#https://docs.python.org/2/library/argparse.html#argparse.ArgumentParser.add_argument

  
def main(i_sheetname,deptName, t_name,regex_column,regex_col_value,replace):
  #i_sheetname="TLog"
  #regex_col_value='^Ticket.*UTC\)$'
  #regex_column='^(2017.*\s\d+):(.*)'
  #replace='\g<1>'
  o_file = "/home/amcdowald/wrksite/Metrics/outputs/{title}-replace.xls".format(title=i_sheetname)
  input_d = create_mega_log(i_sheetname,deptName)
  transf_er2(input_d,i_sheetname,o_file,t_name)
  o_file2 = "/home/amcdowald/wrksite/Metrics/outputs/{title}-sub.xls".format(title=i_sheetname)
  match_and_substitute(i_sheetname,o_file,o_file2,regex_column,regex_col_value,replace)
  
def create_mega_log(i_sheetname,regex_column): 
  start_row=1
  #i_sheetname="TLog"
  #regex_column='Initial Department'
  regex_col_value='Applications Management'
  row_array = []
  mypath = "/home/amcdowald/wrksite/Metrics/inputs/RawMetrics/"
  file_list = getfiles(mypath)
  print("Creating xls file containing")
  #file_list = ["/home/amcdowald/wrksite/Metrics/inputs/RawMetrics/shared_ops_metrics.xls","/home/amcdowald/wrksite/Metrics/inputs/RawMetrics/paas_ops_metrics.xls", "/home/amcdowald/wrksite/Metrics/inputs/RawMetrics/vnsny_ops_metrics.xls"]
  for i_file in file_list:
    rD_book, rD_sheet, wT_book, wT_sheet =xlxs_func(i_sheetname,i_file)
    #this finds the header for
    header_index, header_line = find_header_index(rD_sheet, regex_column)
    end_row=rD_sheet.nrows
    match = re.compile(r'%s' % str(regex_col_value))
    matched_rows = find_matching_rows(rD_sheet,header_index, regex_col_value, end_row)
    row_list = map(lambda row: rD_sheet.row_slice(rowx=row, start_colx=0,end_colx=rD_sheet.ncols), matched_rows)
    for line in row_list:
      row_array.append(line)
  row_list=row_array
  for index, col in enumerate(header_line):
    wT_sheet.write(0,index, col.value)
  write_line(start_row,wT_sheet,row_list)
  o_file = "/home/amcdowald/wrksite/Metrics/outputs/Tlog_Total.xls".format(title=i_sheetname, date=time.asctime( time.localtime(time.time()) ))
  wT_book.save(o_file)
  return o_file

def make_score_card():
  
  input_d="/home/amcdowald/wrksite/Metrics/outputs/Tlog_Total.xls"
  sheetname = "t_TLog"
  regex_column = "Client"
  regex_col_value = ["MasterCard","VNSNY","ULL","Mizuho","IPsoft","sfl","wpp"]
  col_list = ["Client","Respond Time", "Resolve Time","SLA Misses"]
  col_k = ["Respond Time", "Resolve Time"]
  print("Printing scorecards from: \nInput File: %s\n" %input_d)
  rD_book, rD_sheet, wT_book, wT_sheet = xlxs_func(sheetname,input_d)
  for index2, title in enumerate(col_list):
      wT_sheet.write(0,index2,"%s" % title)
  #FOR EACH CLIENT
  for index1, client in enumerate(regex_col_value):
    print("Getting Client: %s\n"%client)
    #Find Clients
    header_index, header_line = find_header_index(rD_sheet, regex_column)  
    matched_rows = find_matching_rows(rD_sheet,header_index, client, rD_sheet.nrows)
    wT_sheet.write(index1+1,0, client)
    nparray=[]
    #GET MEAN FOR Resolve and Respond
    for index2, col in enumerate(col_k):
      #Find Values
      col1 = index2 + 1
      row1 = index1 + 1
      columnNum = find_column(rD_sheet,"%s" % col)
      row_list = map(lambda row: rD_sheet.row_slice(rowx=row, start_colx=columnNum,end_colx=columnNum+1), matched_rows)
      #Get mean
      for row in row_list:
	if not (isinstance(row[0].value, basestring)): 
	  row[0].value = float(row[0].value)
	  nparray.append(row[0].value)
	 # print(row[0].value)
	else:
	  row[0].value = 0
	  nparray.append(float(row[0].value))
      mean = numpy.mean(nparray)
      wT_sheet.write(row1,col1, "{0:.2f}".format(mean))
    #FOR MISS SLA
    columnNum = find_column(rD_sheet,"Miss SLA")
    columnNum2 = find_column(rD_sheet,"Client")
    x = numpy.array(find_matching_rows(rD_sheet,columnNum, "Yes", rD_sheet.nrows))
    y = numpy.array(find_matching_rows(rD_sheet,columnNum2, "%s" % client, rD_sheet.nrows))
    #print(x , y)
    z = numpy.intersect1d(x, y)
    wT_sheet.write(row1,3, z.size)
  wT_book.save("/home/amcdowald/wrksite/Metrics/outputs/totals.xls")
 
  
  
  
  

if __name__=="__main__": 
  try:
    choice = int(sys.argv[1])
    if re.match(r"(0|1|2)", str(choice)):
      try: 
	CallSessionID = int(sys.argv[2])
	sql_calls(choice, CallSessionID)
      except NameError: print("Error with sql_calls function")
    if re.match(r"3", str(choice)):
      try:
	i_file = sys.argv[2]
	sheetname = sys.argv[3]
	out = "/home/amcdowald/wrksite/Metrics/outputs/%s.xls" % sheetname
	transf_er(i_file,sheetname,out)
      except NameError: print("Error with Transform_ER function")
    if re.match(r"4", str(choice)):
      try:
	field = '/home/amcdowald/wrksite/Metrics/outputs/Shared_Tlogs.csv'
	sql_calls(field,4)
      except NameError: print("Error with sql_calls function for Tlogs")
    if re.match(r"5", str(choice)):
      try:
	print("test\n\n")
	#make_score_card()
		
	#main_test() 
	#create_mega_log(TLog",'Initial Department')
	#match_and_substitute(i_sheetname="TLog",i_book="/home/amcdowald/wrksite/Metrics/outputs/Tlog_Total.xls",regex_column='^Ticket.*UTC\)$',regex_col_value='^(2017.*\s\d+):(.*)',replace='\g<1>')
	#match_and_replace(i_sheetname="TLog",i_book="/home/amcdowald/wrksite/Metrics/Applications_Management_2017-08-15.xlsx",regex_column='Initial Department',regex_col_value='Applications Management',replace="bingo")
      except NameError: print("Error with match")
  except NameError:
    print(warnings)

main("ASA Data","Department","Queue Date","^Queue.*Date$",'^(2017.*\s\d+):(.*)','\g<1>')    
#PAAS

#CallSessionID = int(sys.argv[1])# 641167
#choice = int(sys.argv[2])
#print(CallSessionID)

