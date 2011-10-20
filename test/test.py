def computeIntervalTime(start_time, end_time):
    interval_time = None
    start_time_high = int(start_time[:2])
    start_time_low = int(start_time[-2:])
    end_time_high = int(end_time[:2])
    end_time_low = int(end_time[-2:])
        
    if start_time <= end_time:
        if end_time_low < start_time_low:
            end_time_high -= 1
            end_time_low += 60
        interval_time_high = end_time_high - start_time_high
        interval_time_low = end_time_low - start_time_low
        interval_time = "+%02d:%02d" % (interval_time_high, interval_time_low)
        
    if start_time > end_time:
        if start_time_high - end_time_high < 2:
            if start_time_high > end_time_high:
                start_time_high -= 1
                start_time_low += 60
            interval_time_high = start_time_high - end_time_high
            interval_time_low = start_time_low - end_time_low
            interval_time = "-%02d:%02d" % (interval_time_high, interval_time_low)
        else:
            end_time_high += 24
            if end_time_low < start_time_low:
                end_time_high -= 1
                end_time_low += 60
            interval_time_high = end_time_high - start_time_high
            interval_time_low = end_time_low - start_time_low
            interval_time = "+%02d:%02d" % (interval_time_high, interval_time_low)
        
    return interval_time


if __name__ == '__main__':
    a = "20:55"
    b = "20:35"
    print computeIntervalTime(a, b)
    
    a = "21:25"
    b = "20:45"
    print computeIntervalTime(a, b)
    
    a = "20:55"
    b = "21:35"
    print computeIntervalTime(a, b)
    
    a = "20:35"
    b = "21:45"
    print computeIntervalTime(a, b)
    
    a = "20:55"
    b = "00:35"
    print computeIntervalTime(a, b)
    
    a = "22:15"
    b = "00:35"
    print computeIntervalTime(a, b)
    
    a = "22:15"
    b = "22:15"
    print computeIntervalTime(a, b)
    
    
    
    
    
    