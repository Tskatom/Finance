import nltk
import os

negativeDoc  = open("D:/negative.csv").readlines()
stemmer = nltk.stem.snowball.SnowballStemmer('english')
j = []
for l in negativeDoc:
    j.append(stemmer.stem(l.replace("\n","")))

fdist = nltk.FreqDist(j)

out = open("D:/negativeCleaned.txt","w")
for k in fdist:
    out.write(k)
    out.write("\n")
out.close()

positiveDoc = open("d:/positive.csv").readlines()
postiveWords = []
for line in positiveDoc:
    postiveWords.append(stemmer.stem(line.replace("\n","")))

fdist = nltk.FreqDist(postiveWords)
out = open("d:/positiveCleaned.txt","w")
for posWord in fdist:
    out.write(posWord)
    out.write("\n")

out.close()