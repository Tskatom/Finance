import argparse

def parse_args():
    ap = argparse.ArgumentParser("Scrape the content from Bloomberg News and push them to to ZMQ!")
    ap.add_argument('-c','--conf',metavar="CONFIG",type=str,default='../Config/config.cfg',nargs='?',help='the config file path')
    return ap.parse_args()

def main():
    args = parse_args()
    print args.conf
    good = None
    assert good,"--we need good"
    print "hhhh"
    
main()