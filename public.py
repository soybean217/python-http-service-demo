import time

def is_same_month(time1,time2):
    if time1 <= 0 or time2 <= 0 :
        return False
    else:
        if time.strftime("%Y-%m", time.localtime(time1)) == time.strftime("%Y-%m", time.localtime(time2)) :
            return True
        else:
            return False