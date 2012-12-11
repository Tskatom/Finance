import json

file = open("d:/embers/financemodel/termContribution.json")
terms = json.load(file)
print terms
termsJson = {}
termsJson["name"]="clusters"
termsJson["children"]=[]
for children in terms:
    child = {}
    child["name"] = children
    child["children"] = []
    for term in terms[children]:
        obj = {}
        obj["name"] = term
        obj["size"] = float(terms[children][term])
        child["children"].append(obj)
    termsJson["children"].append(child)

print termsJson