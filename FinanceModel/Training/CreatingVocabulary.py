import json
import nltk
from Util import common
from datetime import datetime
import cProfile

def create_vocabulary(feature_num=150):
    "Read the Negative Finance Dictionary"
    negativeFilePath = common.get_configuration("training", "NEGATIVE_DIC")
    negKeywords = json.load(open(negativeFilePath))
    
    
    "Read the Positive Finance Dictionary"
    positiveFilePath = common.get_configuration("training", "POSITIVE_DIC")
    posiKeyWords = json.load(open(positiveFilePath))
    
    "Read the archived news to count the top words"
    BBNewsPath = common.get_configuration("training", "TRAINING_NEWS_FILE")
    
    keyWords = []
    for w in negKeywords:
        keyWords.append(w)
    
    for w in posiKeyWords:
        keyWords.append(w)
        
    print "Over Here"
    
    wordFreq = {}
    flatCount = 0
    for line in open(BBNewsPath,"r"):
        news = json.loads(line)
        flatCount = flatCount + 1
        fdist = news["content"]
        for word in keyWords:
            if word in fdist:
                if word in wordFreq:
                    wordFreq[word] = wordFreq[word] + fdist[word]
                else:
                    wordFreq[word] = fdist[word]
                        
    
    #sorted_obj2 = wordFreq.iteritems()
    sorted_obj2 = sorted(wordFreq.items(), key=lambda x: x[1],reverse=True)
    print sorted_obj2
    "Write the vocabulary list to File"
    vocabularyFile = common.get_configuration("training", "VOCABULARY_FILE")
    output = open(vocabularyFile,"w")
    result_word_list = []
    i = 1
    for word in sorted_obj2:
        if i > feature_num:
            break
        else:
            result_word_list.append(word[0])
            i =  i + 1
    output.write(json.dumps(result_word_list))
    output.flush()
    output.close()        

if __name__ == "__main__":
    print "Creating the Vocabulary Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")
    cProfile.run("create_vocabulary()")
    print "Creating the Vocabulary Start Time: ",datetime.strftime(datetime.now(),"%Y-%m-%d %H:%M:%S")