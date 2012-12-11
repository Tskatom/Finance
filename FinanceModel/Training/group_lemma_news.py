import json
import argparse

"""
Group news by stock market
"""
def arg_parser():
    ap = argparse.ArgumentParser("Transfer the Basis file to Traing suitbable style")
    ap.add_argument('-f',dest="t_file",metavar='BASIC FILE',type=str,help='The path of BASIC file')
    ap.add_argument('-o',dest="o_file",metavar='OUT FILE',type=str,help='The path of OUT file')
    return ap.parse_args()

def group_by_index(t_file):
    stock_news = {}
    for line in open(t_file,"r"):
        news = json.loads(line)
        stock = news["stockIndex"]
        post_date = news["postDate"]
        if stock not in stock_news:
            stock_news[stock] = {}
        if post_date not in stock_news[stock]:
            stock_news[stock][post_date] = []
        stock_news[stock][post_date].append(news)
    return stock_news
        
def main():
    args = arg_parser()
    t_file = args.t_file
    o_file = args.o_file
    articles = group_by_index(t_file)
    with open(o_file,"w") as o_q:
        o_q.write(json.dumps(articles))

if __name__ == "__main__":
    main()        
        
    