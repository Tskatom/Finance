#-*- coding:utf8-*-
import json
import argparse
import sys
import nltk

def arg_parser():
    ap = argparse.ArgumentParser("Transfer the Basis file to Traing suitbable style")
    ap.add_argument('-f',dest="t_file",metavar='BASIC FILE',type=str,help='The path of BASIC file')
    ap.add_argument('-o',dest="o_file",metavar='OUT FILE',type=str,help='The path of OUT file')
    return ap.parse_args()

def main():
    args = arg_parser()
    out_file = open(args.o_file,"w")
    
    i = 0
    with open(args.t_file,"r") as r_f:
        lines = r_f.readlines()
        for line in lines:
            try:
                n = json.loads(line)
                basisEnriched = n["BasisEnrichment"]
                words = [w["lemma"].lower() for w in basisEnriched["tokens"] if w["lemma"] not in [",",".",")","]","(","[","*",";","...",":","&",'"',"'","’"] and not w["lemma"].isdigit()]
                words = [w for w in words if w.encode("utf8") not in nltk.corpus.stopwords.words('english')]
                "remove the stopwords and the Punctuation"
                n["content"] = nltk.FreqDist(words)
                del(n["BasisEnrichment"])
                out_file.write(json.dumps(n))
                out_file.write("\n")
            except Exception as e:
                print sys.exc_info()[0]
                print line
                break
            i = i + 1
            print i
    out_file.flush()
    out_file.close()
if __name__ == "__main__":
    main()
        
    
    