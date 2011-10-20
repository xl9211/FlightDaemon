# coding=utf-8

'''
import os
import sys
current_path = os.getcwd()
base_path = os.path.abspath(os.path.join(current_path, "../../"))
sys.path.append(base_path)
'''


from Spider import Spider
import lxml.html.soupparser 


class Other(Spider):
    
    def __init__(self):
        Spider.__init__(self)
        
        self.ret_val = None
        
        
    def parse(self):
        doc = lxml.html.soupparser.fromstring(self.content)
        tables = doc.xpath("//div[@class='mw-content-ltr']//table")
        self.ret_val = []
        for table in tables:
            rows = table.xpath("tbody/tr")
            for row in rows:       
                columns = row.xpath("td")
                if len(columns) != 0:
                    code = columns[0].text_content().strip()
                    
                    if code != "":
                        company = columns[1].text_content().strip()
                        company_zh = ""
                        company_en = "" 
                        start = company.find('(')
                        if start == -1:
                            start = company.find('（')
                            if start == -1:
                                company_en = company
                            else:
                                end = company.find('）')
                                company_zh = company[:start]
                                company_en = company[start + 1 : end]
                        else:
                            end = company.find(')')
                            company_zh = company[:start]
                            company_en = company[start + 1 : end]                       
                        
                        import re   
                        if company_zh != "" and re.search(r'[^\000-\177]+', company_zh) is None:
                            tmp = company_en
                            company_en = company_zh
                            company_zh = tmp
                        
                        state = columns[2].text_content().strip()
                        
                        print "%s\t%s\t%s\t%s" %(code, company_zh, company_en, state)
                        self.ret_val.append((code, company_zh, company_en, state))
            
                
    def getCompany(self):
        self.content = open('iata_company_code.html')
        self.parse()
        
        return self.ret_val
    
    
    def getAirport(self):
        self.content = open('iata_company_code.html')
        self.parse()
        
        return self.ret_val
    
    
if __name__ == '__main__':     
    test = Other()
    
    test.getCompany()


    