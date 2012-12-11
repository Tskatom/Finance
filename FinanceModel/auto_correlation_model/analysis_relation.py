import json

def analysis():
    print "7 Days correlation"
    bd_list = json.load(open('d:/embers/bd_relation_results_7.json','r'))
    for (k,v) in bd_list.items():
        total_event = len(v)
        p_count = 0.0
        for item in v:
            if len(item['related_events']) > 0:
                p_count = p_count + 1
        print "stock:{} total: {}, p_count:{}, ration:{} ".format(k,total_event,p_count,p_count/total_event)
        

    print "\n5 Days correlation"        
    bd_list = json.load(open('d:/embers/bd_relation_results_5.json','r'))
    for (k,v) in bd_list.items():
        total_event = len(v)
        p_count = 0.0
        for item in v:
            if len(item['related_events']) > 0:
                p_count = p_count + 1
        print "stock:{} total: {}, p_count:{}, ration:{} ".format(k,total_event,p_count,p_count/total_event)
        
    print "\n3 Days correlation"
    bd_list = json.load(open('d:/embers/bd_relation_results.json','r'))
    for (k,v) in bd_list.items():
        total_event = len(v)
        p_count = 0.0
        for item in v:
            if len(item['related_events']) > 0:
                p_count = p_count + 1
        print "stock:{} total: {}, p_count:{}, ration:{} ".format(k,total_event,p_count,p_count/total_event)

if __name__ == "__main__":
    analysis()
        