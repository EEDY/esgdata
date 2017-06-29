# -*- coding: utf-8 -*- 
import  xdrlib ,sys  
import xlwt          

class Excel(object):      
  
  def __init__ (self, name, path='', *args):
    '''book = xlwt.Workbook()'''
    self.__book = xlwt.Workbook()
    self.name = name
    self.__next_rowid = 0
    self.__path = path
    self.__sheet = self.__book.add_sheet(name.encode('gbk'),cell_overwrite_ok=True) #create sheet
    self.style = self.create_style('Times New Roman',220,True)

  def write_line(self, row, row_id=None, column_offset=0, style=None):
    '''
    Write a row which is a list into one line after previous write, or specified 
    line number and column number
    Example:
      book.write_line(['Query Stream Duration', str(datetime.now() - stream_start_time)], 2)
    '''
    if not row_id:
      row_id = self.__next_rowid
    else:
      self.__next_rowid = row_id
    
    style = style
    if not style:
      style = self.style
    
    for i in range(len(row)):
      self.__sheet.write(row_id,i + column_offset,row[i], style)
    
    self.__next_rowid += 1
    return self.__next_rowid

  def next_rowid(self):
    '''Get the next row id which will be wrote'''
    return self.__next_rowid
  
  def write_empty_line(self, amount = 1):
    ''' Some times may need to skip some lines '''
    self.__next_rowid += amount

  def add_sheet(self, name):
    ''' Add a new sheet with some name for the following write '''
    self.__sheet = self.__book.add_sheet(name.encode('gbk'),cell_overwrite_ok=True) #create sheet
    self.__next_rowid = 0

  #@staticmethod  
  def create_style(self, name,height,bold=False):
    style = xlwt.XFStyle() # initialize stype
    
    font = xlwt.Font() # create font object
    font.name = name   # 'Times New Roman'
    if bold:
      font.bold = bold
    font.color_index = 4
    font.height = height
    
    # borders= xlwt.Borders()
    # borders.left= 6
    # borders.right= 6
    # borders.top= 6
    # borders.bottom= 6
    
    style.font = font
    # style.borders = borders
    
    return style
  
  def set_style(self, name, height, bold=False):
    ''' Set the default stype of each write operation '''
    self.style = self.create_style(name, height, bold)

  def save(self):
    import os
    ''' Save the excel workbook after finishing the write operation '''
    self.__book.save(os.path.join(self.__path, self.name+".xls"))
