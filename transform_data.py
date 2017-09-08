#!/usr/bin/python2.7
import csv
import re
import pymysql
import xlrd
import xlwt
import re
import time
import datetime
from os import listdir
from os.path import isfile, join

'''
This separates the UTC Datetime to Date and Time
'''


#take a UTC array separates it into date and time 

def separate_into_two(column,col1_name,col2_name):
  data = []
  col2 = []
  col1 = []
  col1_m = '(2017)-(\d+)-(\d+)(.*)'
  col1_r = '\g<2>-\g<3>-\g<1>'
  col2_m = '(2017)-(\d+)-(\d+)\s(\d+):.*'
  col2_r = '\g<4>:00'
  col1.append("%s" % col1_name )
  col2.append("%s" % col2_name)
  for row in column:
    x = date_to_day(re.sub(r'%s' % col1_m, r'%s' % col1_r, row))
    y = re.sub(r'%s' % col2_m , r'%s' % col2_r, row)
    col1.append(x)#"%s-%s-%s" % (re.group(2),re.group(3),re.group(1)), row)
    col2.append(y)
  data.append((col1,col2)) #data = [] #make a data store
  col2 = data[0][1]
  col1 = data[0][0]
  return col1, col2

def date_to_day(dt):
  month, day, year = (int(x) for x in dt.split('-'))
  DayL = ['Mon','Tues','Wednes','Thurs','Fri','Satur','Sun']
  date = DayL[datetime.date(year,month,day).weekday()] + 'day'
  #Set day, month, year to your value
  #Now, date is set as an actual day, not a number from 0 to 6.
  
  return date

def if_replace(i_file, i_sheetname, columName,match_r, replace,o_file=None):
  """
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
    if re.match("%s" % match_r,row_list[row][header_index].value):
      row_list[row][header_index].value = '%s' % replace
  if (o_file):  
    write_header(rD_sheet,wT_sheet,"%s" % columName )
    write_line(1,wT_sheet,row_list)
    wT_book.save(o_file)
  return header_line ,row_list

def write_header(i_sheet,o_sheet,regex_column):
  header_index, header_line = find_header_index(i_sheet, regex_column) #this finds the header for
  #print(header_line)
  for index, col in enumerate(header_line): 
    o_sheet.write(0,index, col.value) 
    
def xlxs_func(sheetname,input_d ):
  #print(input_d)
  #print(sheetname)
  rD_book = xlrd.open_workbook(input_d) #open our xls file, there's lots of extra default options in this call, for logging etc. take a look at the docs
  rD_sheet = rD_book.sheet_by_name(sheetname)
  wT_book = xlwt.Workbook()
  wT_sheet = wT_book.add_sheet('%s' % sheetname)
  return rD_book, rD_sheet, wT_book, wT_sheet

def transf_er(input_file_address,sheetname,output_file_address,colTitle):  
  #sheetname = "Miss SLA"
  #sheetname = "TLog"
  #input_file_address = "/home/amcdowald/wrksite/Metrics/inputs/Applications_Management_2017-08-21.xlsx"
  output_file_address= output_file_address #"%s" % (output_file_address)
  print("Time Stripper: I will take the Metrics ops UTC for \" %s \" and add date and time\n" % (sheetname))
  #Get time and write
  rD_book, rD_sheet, wT_book, wT_sheet = xlxs_func(sheetname,input_file_address)
  columnNum, title = find_header_index(rD_sheet,'%s'% colTitle)
  column = rD_sheet.col_values(columnNum,start_rowx=1,end_rowx=rD_sheet.nrows)
  
  date, time = separate_into_two(column,"Day","Hour")
  #print(date, time)
  wT_column(wT_sheet,0,date)
  wT_column(wT_sheet,1,time)
  for col in range(0,rD_sheet.ncols-1):
    n_col = col + 2
    column=rD_sheet.col_values(col,start_rowx=0,end_rowx=rD_sheet.nrows)
    wT_column(wT_sheet,n_col,column)  
  wT_book.save(output_file_address)
  print('Saved: ' + output_file_address)
  print("transf_er function complete\n")
  return rD_book, rD_sheet, wT_book, wT_sheet

def wT_column(sheet,columnNum,array):
  for index,value in enumerate(array):
    sheet.write(index,columnNum,value)#time
 
def write_line(start_row,o_sheet,row_line):
  for index, line in enumerate(row_line):
    for index2, col in enumerate(line):
      o_sheet.write(index+start_row,index2,col.value)
 

def find_matching_rows(sheet,headerIndex,regex,end_row):
  match = re.compile(r'%s' % str(regex))
  rows = [rows for rows in range(end_row) if (match.match(sheet.cell(rows,headerIndex).value))]
  return rows

def find_header_index(i_sheet, regex):
  try:
    ticket = re.compile(r'%s' % str(regex))
    header = i_sheet.row_slice(rowx=0, start_colx=0,end_colx=i_sheet.ncols)
    col_index = [index for index, title in enumerate(header) if ticket.match(title.value)]
  except NameError:
    print("Header error")
  return col_index[0], header

def replace_val_in_row(match,col_index,row_list,replace):
  """match = re.compile(r'%s' % str(regex2))
     col_index = header_index[0]
     replace = "hello"""
  #index = col_index[0]
  rows = []
  for value in row_list:
    if match.match(value[col_index].value):
      rep2 = match.sub(replace,value[col_index].value)
      value[col_index].value = rep2
    rows.append(value)
  return rows

def getfiles(mypath):
  onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
  names = []
  for i_file in onlyfiles:
    names.append('{path}{i_file}'.format(path=mypath,i_file=i_file))
  return names

def match_and_substitute(i_sheetname, i_file, o_file, regex_column ,regex_col_value ,replace=None):
  """
  Takes 
  i_sheetname = a sheet name, 
  i_book = an filename address, 
  o_file = an output file address, 
  regex_column = the name of the header you want to match
  regex_col_value = a regex for the value you want to match ex '^(2017.*\s\d+):(.*)'
  
  replace = dependent on regex_col_value ex '\g<1>'
    if replace = None then function will just match
  
  Then writes the matched outputs a list
  
  """
  #static
  start_row=1
  #in variables
  i_book = xlrd.open_workbook("%s" % i_file)
  i_sheet = i_book.sheet_by_name("%s" % i_sheetname)
  end_row=i_sheet.nrows
  #out variables
  o_book = xlwt.Workbook()
  o_sheet = o_book.add_sheet('%s' % i_sheetname )
  match = re.compile(r'%s' % str(regex_col_value))
  header_index, header_line = find_header_index(i_sheet, regex_column) #this finds the header for
   #write header
  for index, col in enumerate(header_line):
    o_sheet.write(0,index, col.value) 
  if re.match(r'.*',str(regex_col_value)):
    print("Matching: {matched} on Column {col}".format(matched=regex_col_value, col=regex_column))
    matched_rows = find_matching_rows(i_sheet,header_index, regex_col_value, end_row)
    row_list = map(lambda row: i_sheet.row_slice(rowx=row, start_colx=0,end_colx=i_sheet.ncols), matched_rows)
    row_line = row_list
  if (not re.match(r'None',str(replace))):
    print("Matching: {matched} on Column {col} Replace: {replaced}\n".format(matched=regex_col_value, col=regex_column, replaced=replace))
    matched_rows = find_matching_rows(i_sheet,header_index, regex_col_value, end_row)
    row_list = map(lambda row: i_sheet.row_slice(rowx=row, start_colx=0,end_colx=i_sheet.ncols), matched_rows)
    row_line = replace_val_in_row(match,header_index,row_list,replace)
  #o_file2 = "/home/amcdowald/wrksite/Metrics/outputs/{title}-sub.xls".format(title=i_sheetname)
  write_line(start_row,o_sheet,row_line)
  print("Saving: {file_o}".format(file_o=o_file))
  print("match_and_substitute function complete\n")
  o_book.save(o_file)
  return  i_book, i_sheet, o_book, o_sheet, row_line

###Experiments
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
    colum_value = row_list[row][header_index].value 
    # ##### input = {colum_value, match_r, replace}
    if re.match("%s" % match_r,colum_value):
      colum_value = '%s' % replace
    if (int(match_r) > colum_value):
      colum_value = function()
    # #####
  if (o_file):  
    write_header(rD_sheet,wT_sheet,"%s" % columName )
    write_line(1,wT_sheet,row_list)
    wT_book.save(o_file)
  return header_line ,row_list
def function():
  return 1