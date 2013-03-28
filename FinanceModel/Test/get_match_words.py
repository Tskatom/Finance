# -*- coding: utf-8 -*-
import nltk
import json
import csv


def get_keywords():
    with open("/home/vic/Downloads/content.txt","r") as f:
        content = f.read()
        tokens = nltk.word_tokenize(content)
        
        tokens = [w.lower() for w in tokens]
        words = [w for w in tokens if w not in nltk.corpus.stopwords.words('english')]
        
        fdist = nltk.FreqDist(words)
        print fdist
        
        "get key words"
        k_json = []
        with open("/home/vic/workspace/data/Negative.csv","r") as ne:
            rd = csv.reader(ne, delimiter=',')
            for r in rd:
                k_json.append(r[0])
        
        with open("/home/vic/workspace/data/Positive.csv","r") as po:
            rd = csv.reader(po, delimiter=',')
            for r in rd:
                k_json.append(r[0])
                
#        k_json = ["gain", "decline", "high", "drop", "cut", "close", "advance", "lead", "concern", "boost", "crisis", "loss", "lose", "slow", "well", "late", "default", "strong", "poor", "win", "bad", "fail", "delay", "weaken", "force", "strengthen", "deficit", "claim", "great", "improve", "press", "ease", "benefit", "weak", "positive", "rebound", "bailout", "refinance", "problem", "opportunity", "able", "question", "recession", "deep", "unemployment", "miss", "hurt", "slowdown", "downgrade", "volatility", "halt", "damage", "bankruptcy", "bolster", "drought", "negative", "disclose", "prevent", "attractive", "antitrust", "break", "challenge", "protest", "shut", "stop", "advantage", "dispute", "lack", "threaten", "investigation", "closing", "complaint", "reject", "suspend", "cancel", "fine", "fire", "sue", "stable", "postpone", "difficult", "opposition", "favor", "unexpectedly", "alliance", "seize", "deny", "improvement", "collapse", "tighten", "stress", "alert", "fraud", "boom", "suffer", "risky", "worsen", "shortage", "strength", "restructuring", "outstand", "surpass", "resolve", "threat", "bridge", "subject", "failure", "bar", "progress", "correct", "doubt", "worry", "conflict", "monopoly", "restructure", "stability", "encourage", "arrest", "popular", "accuse", "fear", "outperform", "profitable", "investigate", "easy", "oppose", "profitability", "erode", "favorable", "drag", "contraction", "warn", "violence", "resign", "shortfall", "success", "achieve", "successful", "deteriorate", "deepen", "defend", "abandon", "argue", "lag", "stabilize", "serious", "turmoil", "criminal", "enable", "despite", "efficiency", "breach", "efficient", "effective", "burn", "confident", "violate", "succeed", "optimistic", "difficulty", "weakness", "wrong", "disappoint", "dismiss", "solve", "litigation", "disaster", "sentence", "undermine", "disrupt", "incident", "lie", "winner", "unable", "adverse", "superior", "burden", "pose", "deterioration", "excessive", "volatile", "penalty", "unrest", "criticize", "severe", "transparency", "allegation", "protester", "guilty", "overshadow", "shock", "violation", "illegal", "victim", "defendant", "crime", "refinancing", "vulnerable", "infringement", "enjoy"]
        
        k_json = [w.lower() for w in k_json]
        print k_json
        
        result = {}
        for k,v in fdist.items():
            if k in k_json:
                result[k] = v
        
        print result
        
get_keywords()        
