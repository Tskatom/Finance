from datetime import datetime
import pytz

def test():
    utc_dt = T_UTC.localize(datetime.utcnow())
    eas_dt = utc_dt.astimezone(T_EASTERN)
    print eas_dt.isoformat()
def main():
    global T_EASTERN,T_UTC
    T_UTC = pytz.utc
    T_EASTERN = pytz.timezone("US/Eastern")
    test()

main()