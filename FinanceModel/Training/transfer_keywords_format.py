import json
import argparse
import nltk

def arg_parser():
    ap = argparse.ArgumentParser("Transfer the Basis file to Traing suitbable style")
    ap.add_argument('-f',dest="t_file",metavar='BASIC FILE',type=str,help='The path of BASIC file')
    ap.add_argument('-o',dest="o_file",metavar='OUT FILE',type=str,help='The path of OUT file')
    return ap.parse_args()

def main():
    args = arg_parser()
    t_file = args.t_file
    o_file = args.o_file
    
    word_list = []
    for line in open(t_file,"r"):
        w_j = json.loads(line)
        word = w_j["BasisEnrichment"]["tokens"][0]["lemma"]
        if word == "2011":
            continue
        word_list.append(word)
    
    fdist = nltk.FreqDist(word_list)
    word_list = [k for k in fdist.keys()]
    print word_list
    
    with open(o_file,"w") as o_q:
        o_q.write(json.dumps(word_list))

if __name__ == "__main__":
    main()    