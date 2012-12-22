import argparse as ap
parse = ap.ArgumentParser()
parse.add_argument('-k',dest="key_id",metavar="KeyId for AWS",type=str,nargs="+",help="The key id for aws")
print parse.parse_args()