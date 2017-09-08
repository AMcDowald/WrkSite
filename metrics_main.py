#!/usr/bin/python2.7
from transform_data import *
import numpy

def main(i_sheetname,department_column_header,regex_column,regex_col_value,replace):
  print("START main () #####################################")
  o_file = "/home/amcdowald/wrksite/Metrics/outputs/{title}-transf_er.xls".format(title=i_sheetname)
  o_file2 = "/home/amcdowald/wrksite/Metrics/outputs/{title}-match_replace.xls".format(title=i_sheetname)
  input_d = create_mega_log(i_sheetname,department_column_header)
  transf_errD_book, transf_errD_sheet, transf_erwT_book, transf_erwT_sheet = transf_er(input_d,i_sheetname,o_file,regex_column)
  i_book, i_sheet, o_book, o_sheet, row_line = match_and_substitute(i_sheetname,o_file,o_file2,regex_column,regex_col_value,replace)
  print("END main ()   #####################################")
  
def create_mega_log(i_sheetname,regex_column,regex_col_value='.*Applications Management|Applications'): 
  start_row=1
  #regex_col_value='.*Enterprise Applications|Enterprise'
  try:
    row_array = []
    mypath = "/home/amcdowald/wrksite/Metrics/inputs/RawMetrics/"
    file_list = getfiles(mypath)
    print("Creating xls file containing data from: \nInput files: {files}\n \nSheetname: {sheet}\n \nLooking for all \"{value}\" in column \"{column}\"".format(files=file_list, sheet=i_sheetname,value=regex_col_value,column=regex_column ))
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
    print("\n\ncreate_mega_log function complete\nCreated file: {outputfile}\n\n".format(outputfile=o_file))
  except NameError:
    print("create_mega_log error. Please close all files open from: %s" % mypath)
  return o_file

def make_score_card():
  
  input_d="/home/amcdowald/wrksite/Metrics/outputs/Tlog_Total.xls"
  sheetname = "TLog"
  regex_column = "Client"
  regex_col_value = ["MasterCard","VNSNY","ULL","Mizuho","MOJ","sfl",'IHG','statefarm']
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
      columnNum , header_line = find_header_index(rD_sheet, col)
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
    columnNum , header_line = find_header_index(rD_sheet,"Miss SLA")
    columnNum2 , header_line = find_header_index(rD_sheet,"Client")
    x = numpy.array(find_matching_rows(rD_sheet,columnNum, "Yes", rD_sheet.nrows))
    y = numpy.array(find_matching_rows(rD_sheet,columnNum2, "%s" % client, rD_sheet.nrows))
    z = numpy.intersect1d(x, y)
    wT_sheet.write(row1,3, z.size)
  wT_book.save("/home/amcdowald/wrksite/Metrics/outputs/totals.xls")
 

#works
#main("ASA Data","Department","^Queue.*Date.*$",'^(2017.*\s\d+):(.*)','\g<1>')
main("TLog","Initial Department","^Ticket\sCreated.*\)",'^(2017.*\s\d+):(.*)','\g<1>')
make_score_card()
print("\n\nDone\n")

