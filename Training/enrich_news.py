import nltk
import json
import cProfile

def enrich_news(file_path):
    g_news = json.load(open(file_path,"r"))
    print g_news.keys()
    stemmer = nltk.stem.snowball.SnowballStemmer('english')
    i = 0
    for k,v in g_news.items():
        s_news = {}
        f_name = "d:/embers/financeModel/output/" + k + "_enriched_BBNews-Group-Stock.json"
        with open(f_name,"w") as o_q:
            for k1,v1 in v.items():
                try:
                    content = v1["content"]
                    tokens = nltk.word_tokenize(content)
                    words = [w.lower() for w in tokens if w not in [",",".",")","]","(","[","*",";","...",":","&",'"'] and not w.isdigit()]
                    words = [w for w in words if w.encode("utf8") not in nltk.corpus.stopwords.words('english')]
                    stemmedWords = [stemmer.stem(w) for w in words]
                    fdist=nltk.FreqDist(stemmedWords)
                    v1["content"] = fdist
                except:
                    v1["content"] = {}
                    continue
            s_news[k] = v
            o_q.write(json.dumps(s_news))
        print "Done: ",k
    "Write words back to file"
    with open("d:/embers/financeModel/output/enriched_BBNews-Group-Stock.json","w") as out_q:
        out_q.write(json.dumps(g_news))
    
def main():
    f = "d:/embers/financeModel/output/BBNews-Group-Stock.json"
    enrich_news(f)
    pass

if __name__ == "__main__":
    main()