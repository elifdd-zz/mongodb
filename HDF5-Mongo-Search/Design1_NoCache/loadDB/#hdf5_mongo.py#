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
    
###########################################################
def writeDataRecord(grp,list_of_attrs, dbhost, dbport,dbname, fname, h5name,rectype, collname, groupname, subgroupname, sitem):
    
    att_type_list=list()
    connection = Connection(dbhost, dbport)
    db = connection[dbname]
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

    f = h5py.File(inf, 'r') 
    groups=list(f)
    list_of_attrs = list(f.attrs)
    
    for dirs in groups:
        #ASK WHY o.
        opdir="o."+dirs;
        
        groupname=dirs
        subgroupname=""
        item=""
        subgroups=f[dirs]
        list_of_attrs = list(subgroups.attrs)
        metadatafilename=opdir+"/"+dirs+".sct"
        
        metaname=dirs+".sct"
        writeDataRecord(subgroups,list_of_attrs,dbhost, dbport,dbname,metadatafilename,inf, mtype,METACOLLNAME, groupname, subgroupname, metaname)

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
                    
                    writeDataRecord(dataObj,obj_att_list,dbhost, dbport,dbname,metadatafilename,inf, itype, IMAGECOLLNAME,groupname, subgroupname, dset)

            else:
#                print "item not tif"
                #os.mkdir(os.path.join(opdir,str(item)))
                tilelist=subgroups[item]
                list_of_attrs = list(tilelist.attrs)
                metadatafilename=opdir+"/"+item+"/"+item+".sct"

                subgroupname=item
                metaname=item+".sct"
                writeDataRecord(tilelist,list_of_attrs,dbhost, dbport,dbname,metadatafilename, inf, mtype, METACOLLNAME,groupname, subgroupname, metaname)
                tilefilelist=list(tilelist)

                for fimage in tilefilelist:
                    if 'tif' in str(fimage):
#                        print "tif in subdir"
                        dataObj = tilelist[fimage]
                        obj_att_list= list(dataObj.attrs)

                        
                        subgroupname=item
                        writeDataRecord(dataObj,obj_att_list,dbhost, dbport,dbname,metadatafilename,inf, itype, IMAGECOLLNAME,groupname, subgroupname, fimage)
                        for dset in dict(dataObj):
                            dataname=subgroups.name+"/"+item+"/"+fimage+"/"+dset

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
    if (len(sys.argv) < 5):
            print("Usage python hdf5_mongo.py <filename><hostname><portnum><dbname>")
    else:
        print "Starting to load HDF5 files..."
        loadFiles(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4])
        print "Done."
        #opdir = parseHDF5(sys.argv[1], sys.argv[2], int(sys.argv[3]), sys.argv[4])

if __name__ == '__main__':
    sys.exit(main())


        
