import numpy as np
import pymongo
import h5py
import sys, time
import os
import re

from libtiff import TIFFimage, TIFFfile
from pymongo import Connection

METACOLLNAME= "metadatafile"
IMAGECOLLNAME= "image"
ATT_TYPECOLL= "attributes"

mtype="type_metadata"
itype="type_image"

num_of_recs=0

###########################################################                                                                                                                    
def addAttsToDoc(h5doc, grp, list_of_attrs):
    j=1
    for i in list_of_attrs:
        # so that it is all string in DB                                                                                                                                       
        if isinstance(grp.attrs[i], str ):
            h5doc[i]=grp.attrs[i].strip()
        else:
            h5doc[i]=str(grp.attrs[i])
        
###########################################################
def storeRecord(h5doc, dbhost, dbport,dbname):
    connection = Connection(dbhost, dbport)
    db = connection[dbname]
    incol = db[METACOLLNAME]
    incol.insert(h5doc, safe=True)
    h5doc ={}    

###########################################################
def writeDataRecord(grp,list_of_attrs, dbhost, dbport,dbname, fname, h5name,rectype, collname, groupname, subgroupname, sitem):
    
    att_type_list=list()
    connection = Connection(dbhost, dbport)
    db = connection[dbname]
    #gname= re.sub('[/!@#$]', '', grp.name)
    incol = db[collname]
    typecol=db[ATT_TYPECOLL]
    
    h5doc={"h5name" : h5name, "metafilename" : fname, "group": groupname, "subgroup": subgroupname, "itemname": sitem, "h5rectype": rectype}
    
    j=1
    for i in list_of_attrs:
        # so that it is all string in DB
        if isinstance(grp.attrs[i], str ):
            h5doc[i]=grp.attrs[i].strip()
        else:
            h5doc[i]=str(grp.attrs[i])
        j+=1
        typecol.save({"_id":i, "type":rectype}, manipulate=True, safe=True)

    incol.insert(h5doc, safe=True)
    h5doc ={}

###########################################################
def parseHDF5(inf,dbhost, dbport,dbname):
    global num_of_recs

    f = h5py.File(inf, 'r') 
    groups=list(f)
    list_of_attrs = list(f.attrs)

    h5doc={"h5name" : inf}  

    for dirs in groups:
        #ASK WHY o.
        opdir="o."+dirs;
        #os.mkdir(opdir);
        groupname=dirs
        subgroupname=""
        item=""
        subgroups=f[dirs]
        list_of_attrs = list(subgroups.attrs)
        metadatafilename=opdir+"/"+dirs+".sct"

        metaname=dirs+".sct"
        h5doc["metafilename"]=metadatafilename
###        writeDataRecord(subgroups,list_of_attrs,dbhost, dbport,dbname,metadatafilename,inf, mtype,METACOLLNAME, groupname, subgroupname, metaname)
        addAttsToDoc(h5doc, subgroups, list_of_attrs)

        h5doc["group"]= groupname
        h5doc["subgroup"]= subgroupname
        h5doc["itemname"]=metaname
        num_of_recs+=1
        h5doc["_id"]=num_of_recs
        storeRecord(h5doc, dbhost, dbport,dbname)

        subgrouplist=list(subgroups)
        for item in subgrouplist:
            
            if 'tif' in str(item):
#                print "Tiff file..."
                dataObj = subgroups[item]

                obj_att_list = list(dataObj.attrs)

                for dset in dict(dataObj):
                    dataname=subgroups.name+"/"+item+"/"+dset
                    data = f[dataname].value
                    subgroupname=subgroups.name
                    
##                    writeDataRecord(dataObj,obj_att_list,dbhost, dbport,dbname,metadatafilename,inf, itype, IMAGECOLLNAME,groupname, subgroupname, dset)
                    h5doc["metafilename"]=metadatafilename
                    addAttsToDoc(h5doc, dataObj,obj_att_list)
                    h5doc["group"]= groupname
                    h5doc["subgroup"]= subgroupname
                    h5doc["itemname"]= dset
                    num_of_recs+=1
                    h5doc["_id"]=num_of_recs
                    storeRecord(h5doc, dbhost, dbport,dbname)

            else:
#                print "item not tif"
                #os.mkdir(os.path.join(opdir,str(item)))
                tilelist=subgroups[item]
                list_of_attrs = list(tilelist.attrs)
                metadatafilename=opdir+"/"+item+"/"+item+".sct"
#                print "Metafile name : ", metadatafilename
                subgroupname=item
                metaname=item+".sct"
#                writeDataRecord(tilelist,list_of_attrs,dbhost, dbport,dbname,metadatafilename, inf, mtype, METACOLLNAME,groupname, subgroupname, metaname)
                addAttsToDoc(h5doc, tilelist,list_of_attrs)
                h5doc["group"]= groupname
                h5doc["subgroup"]= subgroupname
                h5doc["itemname"]= metaname
                h5doc["metafilename"]=metadatafilename
                num_of_recs+=1
                h5doc["_id"]=num_of_recs
                storeRecord(h5doc, dbhost, dbport,dbname)
                tilefilelist=list(tilelist)
                #print "------------------------- tilefilelist: ", tilefilelist
                for fimage in tilefilelist:
                    if 'tif' in str(fimage):
#                        print "tif in subdir"
                        dataObj = tilelist[fimage]
                        obj_att_list= list(dataObj.attrs)
                        
                        subgroupname=item
##                        writeDataRecord(dataObj,obj_att_list,dbhost, dbport,dbname,metadatafilename,inf, itype, IMAGECOLLNAME,groupname, subgroupname, fimage)
                        addAttsToDoc(h5doc, dataObj, obj_att_list)
                        h5doc["group"]= groupname
                        h5doc["subgroup"]= subgroupname
                        h5doc["itemname"]= fimage
                        num_of_recs+=1
                        h5doc["_id"]=num_of_recs
                        storeRecord(h5doc, dbhost, dbport,dbname)

    return opdir           

###########################################################
def loadFiles(dir,dbhost, dbport,dbname):
    if os.path.isdir(dir):
        basedir = dir
        subdirlist = []
        for item in os.listdir(dir):
            if os.path.isfile(os.path.join(basedir,item)):
                if item.endswith(".h5"):
                    print "- Loading file: ", os.path.join(basedir,item)
                    #item
                    parseHDF5(os.path.join(basedir,item),dbhost, dbport,dbname)
                else:
                    subdirlist.append(os.path.join(basedir, item))
        for subdir in subdirlist:
            loadFiles(subdir)
    else:
        if dir.endswith(".h5"):
            print "- Loading file: ", dir
            parseHDF5(dir,dbhost, dbport,dbname)
                    
###########################################################
def main():
    global num_of_recs
    if (len(sys.argv) < 5):
            print("Usage python hdf5_mongo.py <filename><hostname><portnum><dbname>")
    else:
        print "Starting to load HDF5 files..."
        num_of_recs=0
        loadFiles(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4])
        print "Done."

if __name__ == '__main__':
    sys.exit(main())


        
