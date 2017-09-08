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
def separate_UTC(column):
  data = []
  time = []
  date = []
  date.append("Date")
  time.append("Time")
  for row in column:
    date.append(re.sub(r'(2017)-(\d+)-(\d+)(.*)', r'\g<2>-\g<3>-\g<1>', row))#"%s-%s-%s" % (re.group(2),re.group(3),re.group(1)), row)
    time.append(re.sub(r'(2017)-(\d+)-(\d+)\D(.*)', r'\g<4>', row))
  data.append((date,time)) #data = [] #make a data store
  time = data[0][1]
  date = data[0][0]
  return date, time

def separate_UTC2(column):
  data = []
  time = []
  date = []
  date.append("Date")
  time.append("Time")
  for row in column:
    date.append(date_to_day(re.sub(r'(2017)-(\d+)-(\d+)(.*)', r'\g<2>-\g<3>-\g<1>', row)))#"%s-%s-%s" % (re.group(2),re.group(3),re.group(1)), row)
    time.append(re.sub(r'(2017)-(\d+)-(\d+)\D(\d+):.*', r'\g<4>', row))
  data.append((date,time)) #data = [] #make a data store
  time = data[0][1]
  date = data[0][0]
  return date, time

def date_to_day(dt):
  month, day, year = (int(x) for x in dt.split('-'))
  DayL = ['Mon','Tues','Wednes','Thurs','Fri','Satur','Sun']
  date = DayL[datetime.date(year,month,day).weekday()] + 'day'
  #Set day, month, year to your value
  #Now, date is set as an actual day, not a number from 0 to 6.
  print(date)
  return date

#write an array to a column
def wT_column(sheet,columnNum,array):
  for index,value in enumerate(array):
    sheet.write(index,columnNum,value)#time 
#create read and write xlxs object 
def xlxs_func(sheetname,input_d ):
  #print(input_d)
  #print(sheetname)
  rD_book = xlrd.open_workbook(input_d) #open our xls file, there's lots of extra default options in this call, for logging etc. take a look at the docs
  rD_sheet = rD_book.sheet_by_name(sheetname)
  wT_book = xlwt.Workbook()
  wT_sheet = wT_book.add_sheet('%s' % sheetname)
  return rD_book, rD_sheet, wT_book, wT_sheet
#find the column number of a column
def find_column(sheet,colName):
  row_s = sheet.row_slice(rowx=0, start_colx=0,end_colx=sheet.ncols)
  print(colName)
  index = [index for index, list1 in enumerate(row_s) if (list1.value == '%s' % colName)]
  #print(row_s[index[0]],colName)
  return index[0]


def transf_er(input_d,sheetname,output_d,colTitle):  
  #sheetname = "Miss SLA"
  #sheetname = "TLog"
  #input_d = "/home/amcdowald/wrksite/Metrics/inputs/Applications_Management_2017-08-21.xlsx"
  output_d= output_d #"%s" % (output_d)
  print("Time Stripper: I will take the Metrics ops UTC for \" %s \" and add date and time " % (sheetname))
  #Get time and write
  rD_book, rD_sheet, wT_book, wT_sheet = xlxs_func(sheetname,input_d)
  columnNum = find_column(rD_sheet,'%s'% colTitle)
  column = rD_sheet.col_values(columnNum,start_rowx=1,end_rowx=rD_sheet.nrows)
  date, time = separate_UTC(column)
  #print(date, time)
  wT_column(wT_sheet,0,date)
  wT_column(wT_sheet,1,time)
  for col in range(0,rD_sheet.ncols-1):
    n_col = col + 2
    column=rD_sheet.col_values(col,start_rowx=0,end_rowx=rD_sheet.nrows)
    wT_column(wT_sheet,n_col,column)  
  wT_book.save(output_d)
  print('Saved: ' + output_d)
  #("/home/amcdowald/wrksite/Metrics/outputs/App.xlsx")
  
def transf_er2(input_d,sheetname,output_d,colTitle):  
  #sheetname = "Miss SLA"
  #sheetname = "TLog"
  #input_d = "/home/amcdowald/wrksite/Metrics/inputs/Applications_Management_2017-08-21.xlsx"
  output_d= output_d #"%s" % (output_d)
  print("Time Stripper: I will take the Metrics ops UTC for \" %s \" and add date and time " % (sheetname))
  #Get time and write
  rD_book, rD_sheet, wT_book, wT_sheet = xlxs_func(sheetname,input_d)
  columnNum = find_column(rD_sheet,'%s'% colTitle)
  column = rD_sheet.col_values(columnNum,start_rowx=1,end_rowx=rD_sheet.nrows)
  date, time = separate_UTC2(column)
  #print(date, time)
  wT_column(wT_sheet,0,date)
  wT_column(wT_sheet,1,time)
  for col in range(0,rD_sheet.ncols-1):
    n_col = col + 2
    column=rD_sheet.col_values(col,start_rowx=0,end_rowx=rD_sheet.nrows)
    wT_column(wT_sheet,n_col,column)  
  wT_book.save(output_d)
  print('Saved: ' + output_d)
  
def wT_column(sheet,columnNum,array):
  for index,value in enumerate(array):
    sheet.write(index,columnNum,value)#time
    
def find_header_index(i_sheet, regex):
  try:
    ticket = re.compile(r'%s' % str(regex))
    header = i_sheet.row_slice(rowx=0, start_colx=0,end_colx=i_sheet.ncols)
    col_index = [index for index, title in enumerate(header) if ticket.match(title.value)]
  except NameError:
    print("Header error")
  
  return col_index[0], header

def find_matching_rows(sheet,headerIndex,regex,end_row):
  match = re.compile(r'%s' % str(regex))
  rows = [rows for rows in range(end_row) if (match.match(sheet.cell(rows,headerIndex).value))]
  return rows

def replace_val_in_row(match,col_index,row_list,replace):
  """match = re.compile(r'%s' % str(regex2))
     col_index = header_index[0]
     replace = "hello"""
  #index = col_index[0]
  rows = []
  for index in col_index:
    for value in row_list:
      if match.match(value[index].value):
	value[index].value = replace
      rows.append(value)
  return rows

def write_line(start_row,o_sheet,row_line):
  for index, line in enumerate(row_line):
    for index2, col in enumerate(line):
      o_sheet.write(index+start_row,index2,col.value)

def match_and_replace(i_sheetname, i_book,regex_column,regex_col_value,replace=None):
  start_row=1
  #In
  i_book = xlrd.open_workbook("%s" % i_book)
  i_sheet = i_book.sheet_by_name("%s" % i_sheetname)
  end_row=i_sheet.nrows
  #Out
  o_book = xlwt.Workbook()
  o_sheet = o_book.add_sheet('t_%s' % i_sheetname )
  
  match = re.compile(r'%s' % str(regex_col_value))
  header_index, header_line = find_header_index(i_sheet, regex_column) #this finds the header for
   #write header
  for index, col in enumerate(header_line):
    o_sheet.write(0,index, col.value) 
  if re.match(r'.*',str(regex_col_value)):
    matched_rows = find_matching_rows(i_sheet,header_index, regex_col_value, end_row)
    row_list = map(lambda row: i_sheet.row_slice(rowx=row, start_colx=0,end_colx=i_sheet.ncols), matched_rows)
    row_line = row_list
  if (not re.match(r'None',str(replace))):
    matched_rows = find_matching_rows(i_sheet,header_index, regex_col_value, end_row)
    row_list = map(lambda row: i_sheet.row_slice(rowx=row, start_colx=0,end_colx=i_sheet.ncols), matched_rows)
    row_line = replace_val_in_row(match,header_index,row_list,replace)
  write_line(start_row,o_sheet,row_line)
  o_file = "/home/amcdowald/wrksite/Metrics/outputs/{title}-{date}.xls".format(title=i_sheetname, date=time.asctime( time.localtime(time.time()) ))
  o_book.save(o_file)
  return o_file

#Separate



def getfiles(mypath):
  onlyfiles = [f for f in listdir(mypath) if isfile(join(mypath, f))]
  names = []
  for i_file in onlyfiles:
    names.append('{path}{i_file}'.format(path=mypath,i_file=i_file))
  return names

#TEST
def replace_val_in_row2(match,col_index,row_list,replace):
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

      
def match_and_substitute(i_sheetname, i_book, o_file, regex_column,regex_col_value,replace=None):
  start_row=1
  #In
  i_book = xlrd.open_workbook("%s" % i_book)
  i_sheet = i_book.sheet_by_name("%s" % i_sheetname)
  end_row=i_sheet.nrows
  #Out
  o_book = xlwt.Workbook()
  o_sheet = o_book.add_sheet('t_%s' % i_sheetname )
  
  match = re.compile(r'%s' % str(regex_col_value))
  header_index, header_line = find_header_index(i_sheet, regex_column) #this finds the header for
   #write header
  for index, col in enumerate(header_line):
    o_sheet.write(0,index, col.value) 
  if re.match(r'.*',str(regex_col_value)):
    matched_rows = find_matching_rows(i_sheet,header_index, regex_col_value, end_row)
    row_list = map(lambda row: i_sheet.row_slice(rowx=row, start_colx=0,end_colx=i_sheet.ncols), matched_rows)
    row_line = row_list
  if (not re.match(r'None',str(replace))):
    matched_rows = find_matching_rows(i_sheet,header_index, regex_col_value, end_row)
    row_list = map(lambda row: i_sheet.row_slice(rowx=row, start_colx=0,end_colx=i_sheet.ncols), matched_rows)
    row_line = replace_val_in_row2(match,header_index,row_list,replace)
  print(row_line)
  write_line(start_row,o_sheet,row_line)
  o_file = o_file#"/home/amcdowald/wrksite/Metrics/outputs/{title}-replace.xlsx".format(title=i_sheetname)#, date=time.asctime( time.localtime(time.time()) ))
  o_book.save(o_file)
  return o_file