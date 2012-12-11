from Util import common

def main():
    vocabularyFile = common.get_configuration("training", "VOCABULARY_FILE")
    key_list = []
    with open(vocabularyFile,"r") as rf:
        lines = rf.readlines()
        for line in lines:
            line = line.strip()
            key_list.append(line)
    
    print key_list

if __name__ == "__main__":
    main()