import sys, time
import pymongo

from bson import BSON
from pymongo import Connection

#########################################################
class Comparisons:
    ERR, GT, LT, EQ, NE, GTE, LTE, AA, OO = range(9)

HELP="help"
EXIT="bye"
GREATERTHAN="gt"
LESSTHAN="lt"
EQUALS="eq"
NOTEQUALS="ne"
GEQUALS="gte"
LEQUALS="lte"
AND="and"
OR="or"
SQSIZE=3

DBHOST=""
DBPORT=""
DBNAME=""

METACOLLNAME= "metadatafile"
IMAGECOLLNAME= "image"
ATTTYPECOLL= "attributes"

METAATT="type_metadata"
IMAGEATT="type_image"

num_of_recs=0
#########################################################
def printExQs():
    print "   Bad query!"
    print "   Example: <x gt 16 and y lt 34>, <x gt 16>"

#########################################################
def checkKeyWord(k):
    if k.strip() == GREATERTHAN:
        return Comparisons.GT

    elif k.strip() == LESSTHAN:
                return Comparisons.LT

    elif k.strip() == EQUALS:
        return Comparisons.EQ

    elif k.strip() == NOTEQUALS:
        return Comparisons.NE
    
    elif k.strip() == GEQUALS:
        return Comparisons.GTE

    elif k.strip() == LEQUALS:
        return Comparisons.LTE

    else:
        return Comparisons.ERR

#########################################################    
def checkAndOr(k):
    if k.strip() == AND:
        return Comparisons.AA

    elif k.strip() == OR:
        return Comparisons.OO

    else:
        return Comparisons.ERR

#########################################################
def queryCheck(statlist, joinlist):
    for slist in statlist:
        if checkKeyWord(slist[1]) == Comparisons.ERR:
            printExQs()
            return Comparisons.ERR
            
    for k in joinlist:
        if checkAndOr(k) == Comparisons.ERR:
            printExQs()
            return Comparisons.ERR
    return

#########################################################
# returns if attribute is a metafile or image type
def getAttributeType(att,coll):
    attobj=coll.find_one({"_id":att})
    type = None
    if attobj != None:
        type = attobj["type"]
    return type

#########################################################
# create dicts one for metafile attributes one for image attributes
# returns a list of these dict
def groupAttributesInSearchGroup(statlist, attcoll):
    dictlist={} #list()
    
    for slist in statlist:
        addToDict(dictlist,slist)
        
    return dictlist

#########################################################
#turns a search term to document type and adds to the dict
#returns dictionary
def addToDict(dict, slist):
     if slist[1].strip() == EQUALS:
         dict[slist[0]] = slist[2]
     else:
         dict[slist[0]]={"$"+slist[1]:slist[2]}
     return dict

#########################################################
# returns list of lists 
# each list is to be formed a search on db
# each list is seperated by "OR" in the search query
def formSearchQueryLists(statlist, joinlist):
    if queryCheck(statlist, joinlist) != Comparisons.ERR:
        querypartslist=list()
        temp=list()
        i=0
        for slist in statlist:
            temp.append(slist)
            if (joinlist.__len__()>0) and (i<joinlist.__len__()):
                if joinlist[i] == OR:
                    querypartslist.append(temp)
                    temp=list()                
                i+=1
        querypartslist.append(temp)
        return querypartslist

#########################################################      
def procQuery(statlist, joinlist):
    #first validate query sytax
    if queryCheck(statlist, joinlist) != Comparisons.ERR:
    
        connection = Connection(DBHOST,int(DBPORT))
        db = connection[DBNAME]
        metacoll=db[METACOLLNAME]
        imagecoll=db[IMAGECOLLNAME]
        attcoll=db[ATTTYPECOLL]

        queryORedParts= formSearchQueryLists(statlist,joinlist)
        for part in queryORedParts:
            print ">>>"
            attdictlist = groupAttributesInSearchGroup(part, attcoll)
            #for attdict in attdictlists:
            if attdictlist != None:
                queryDBforAttDicts(attdictlist, metacoll, imagecoll)  
        return
    
    else:
        return

#########################################################
# query the DB for the documents created based on the user query
# each sub query has metadata and image attribute dics 
def queryDBforAttDicts(dictlist, metacoll, imagecoll):
    global num_of_recs

    matches = None

    matches = search(metacoll, dictlist)

    if matches.count >0:
        for match in matches:
            print " >> Group: ",match["group"]," -Subgroup: ",match["subgroup"]," -Item: ",match["itemname"]
            num_of_recs+=1


#########################################################                                                                
def search(coll, dict):
    #print "=========================================="
    h5s = coll.find(dict, {"h5name":1, "metafilename":1, "group":1, "subgroup":1, "itemname":1})
    if h5s.count() == 0:
        print "No item found for the condition: "
        print " ",dict

    return h5s

#########################################################                                                                 
def parseUserInput(word_list, num_of_words, num_of_stats, num_of_join_stats, stats_list, join_list):
    add_join=0
    for i in range(0,num_of_stats):
        temp=list()
        if i>0:
            add_join+=1
        track=0
        for j in range(0,SQSIZE):
            temp.append(word_list[i*SQSIZE +j+ add_join])
            track=i*SQSIZE +j+add_join
        stats_list.append(temp)
    
    #print "Query statements: ", stats_list   
    for k in range(1,num_of_join_stats+1):
        join_list.append(word_list[k*(SQSIZE+1) -1])
     #print "Join statements: ", join_list

    return
    
#########################################################                                                                                                                      
def printHelp():
    print "> Type <help> for help"
    print "> Type <bye> to exit"

    print "> Keywords to be used in query : "
    print "  >> gt  : GREATERTHAN"
    print "  >> lt  : LESSTHAN"
    print "  >> eq  : EQUALS"
    print "  >> ne  : NOTEQUALS"
    print "  >> gte : GRETER THAN OR EQUALS"
    print "  >> lte : LESS THAN OR EQUALS"
    print "  >> and : AND"
    print "  >> or  : OR"


#########################################################                
def main():
    global num_of_recs
    if (len(sys.argv) < 4):
        print("Usage python main_query.py <hostname><portnum><dbname>")
        return
    
    global DBHOST
    DBHOST=sys.argv[1]
    global DBPORT
    DBPORT=sys.argv[2]
    global DBNAME
    DBNAME=sys.argv[3]
    
    print "> Please enter query: "
    
    for line in iter(sys.stdin.readline, ""):
        word_list=line.strip().split(" ")
        num_of_words = word_list.__len__()
        num_of_stats = (num_of_words/(SQSIZE+1))+1
        num_of_join_stats=num_of_stats-1
#       num_of_join_stats = num_of_words%SQSIZE

        if num_of_words>=3 :
            print "Your Query: ", line

            if num_of_words != (num_of_stats*SQSIZE+num_of_join_stats):
                printExQs()

#            print "Number of words in query: ", num_of_words
#            print "Number of search statements  in query: ", num_of_stats
#            print "Number of search statement joins  in query: ", num_of_join_stats

            else:
                stats_list=list()
                join_list=list()
                parseUserInput(word_list, num_of_words, num_of_stats, num_of_join_stats, stats_list, join_list)

            ## done initializing lists
            ## do work
                num_of_recs=0
                t0 = time.time()
                procQuery(stats_list, join_list)
                t1 = time.time()
                print "Number of records found: ", num_of_recs
                print "Time for the query: ", (t1-t0)

        else:
		if num_of_words==1:
                    if word_list[0].strip() == EXIT :
                	print "bye!"
                	return
                    elif word_list[0].strip() == HELP :
                        printHelp()
                    else:
                        printExQs()
                else:        
                    printExQs()
                
        print " "
        print "> Please enter query: "

if __name__ == '__main__':
        sys.exit(main())
