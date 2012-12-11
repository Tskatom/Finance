import xlwt

def main():
    wbk = xlwt.Workbook()
    sheet = wbk.add_sheet('report')
    sheet.write(0,1,'test text')
    wbk.save('test1.xls')
if __name__=="__main__":
    print main()