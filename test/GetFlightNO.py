import traceback
import urllib2
import lxml.html.soupparser


def fetch(url):
    try:
        if url is not None:
            response = urllib2.urlopen(url)
            html = response.read()
            return html
        else:
            return -2
    except:
        traceback.print_exc()
        
        return -1
    
if __name__ == "__main__":
    html = fetch("http://www.feeyo.com/flightsearch_number.htm")
    
    doc = lxml.html.soupparser.fromstring(html)
        
    info = doc.xpath("//div[@class='cityli']/p/a")
    
    file = open('flight_no.txt', 'wb')
    count = 0
    do = False
    for one in info:
        fn = one.text_content().strip()
        '''
        if fn == "CA4717":
            do = True
        if do:
            print "%s %s %s" %(count, fn, fetch("http://127.0.0.1:10001/getFixFlightInfoByFlightNO/%s" % (fn)))
        '''
        file.write(fn + '\n')
        count += 1
    file.close()