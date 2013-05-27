#!/usr/bin/python

import os
import re
import struct
from hashlib import sha256

class Database(object):
    def __init__(self,dir='./db/',*params):
        if(dir != None):
            self.dbDir = dir
        else:
            self.dbDir = '.'

        if(params != None):
            self.useDatabase(params[0])

    def listDatabases(self):
        '''List all databases'''
        dirList = []
        for paths,dirs,files in os.walk('/Users/chalovich/Projects/Caviar/KVS/db'):
            for d in dirs:
                dirList.append(d)
        return dirList

    def useDatabase(self, db):
        '''Choose a Database to work with'''
        dbPath = self.dbDir + '/' + db
        if(os.path.isdir(dbPath)):
            os.chdir(self.dbDir + '/' + db)
            self.currentDb = db
        else:
            raise DatabaseError('Database Doesn\'t exist')

        return self.getDbFiles()

    def closeDatabase(self, files):
        '''Close Database'''
        if(self.currentDb != None):
            os.chdir('../../')
            self.currentDb = None
            self.files = None
        else:
            raise DatabaseError("We're not currently using a database")

    def listDbFiles(self):
        '''List all files that make up this database'''
        if(self.currentDb != None):
            for paths,dirs,files in os.walk('.'):
                for f in files:
                    if(re.match(self.currentDb + r'_data\.\d', f)):
                        yield f
        else:
            raise DatabaseError("We're not currently using a database")

    def getDbFiles(self):
        self.files = []
        if(self.currentDb != None):
            for paths,dirs,files in os.walk('.'):
                for f in files:
                    self.files.append(DbFile(f,self.currentDb))
        return self.files

    def countDbFiles(self):
        '''Count number of files that comprise this DB'''
        return len(self.getDbFiles()) 

class DbFile(object):
    def __init__(self,fileName,db):
        self.file = open(fileName, 'r')
        self.FILESIZE = os.path.getsize(fileName)
        self.MAGICLEN = 8
        self.MAGICOFFSET = 0
        self.UUIDLEN = 64 
        self.UUIDOFFSET = 8
        self.INDEXLEN = 4
        self.INDEXOFFSET = 72 
        self.FIELDS = ('Magic','UUID','Index')
        self.FILENAME = fileName
        self.DB = db
        self.NOTFINISHED = 48
        self.fileOffset = 0
        self.blocks = []

        self.getBlocks()

    def getBlocks(self):
        i = 0
        while self.FILESIZE > self.fileOffset:
            self.blocks.append(Block(self.fileOffset,self.file))
            blockLen = self.blocks[i]().BLOCKLENGTH
            i = i + 1
            self.fileOffset = self.fileOffset + blockLen

class Block(object):
    def __init__(self,filePos,myFile):
        self.FILE = myFile
        self.VERSIONLEN = 4
        self.TIMESTAMPLEN = 8
        self.HASHLEN = 64
        self.LENGTHLEN = 4
        self.LENGTHOFFSET = self.VERSIONLEN + self.TIMESTAMPLEN + self.HASHLEN
        self.BLOCKORDER = ('version','timestamp','hash','length','json')
        self.filePos = filePos
        self.JSONLEN = self.getJSONLength()
        self.BLOCKLENGTH = self.getTotalBlockLength()

    def getJSONLength(self):
        self.FILE.seek(self.filePos + self.LENGTHOFFSET)
        bits = self.FILE.read(self.LENGTHLEN)
        return struct.unpack("<I", bits)[0]

    def getTotalBlockLength(self):
        return int(self.VERSIONLEN) + int(self.TIMESTAMPLEN) + int(self.HASHLEN) + int(self.LENGTHLEN) + int(self.JSONLEN)

    def setFilePos(pos):
        self.filePos = pos

class Validator():
    def __init__(self):
        self.db = Database()

    def validateHeader(self):
        '''Begin validating all Fields'''
        while True:
            for f in self.FIELDS:
                if getattr(self, 'validate%s' % f)() == False:
                    raise ValidationError("File %s does not have a valid header. The %s is broken." % (self.FILENAME, f))
                    break

    def validateMagic(self):
        self.file.seek(self.MAGICOFFSET)
        return self.file.read(self.MAGICLEN) == 'caviar01'

    def validateUUID(self):
        self.file.seek(self.UUIDOFFSET)
        UUID = self.file.read(self.UUIDLEN)
        hexUUID = sha256(self.DB).hexdigest()
        return UUID == hexUUID

    def validateIndex(self):
        self.file.seek(self.INDEXOFFSET)
        fileName, fileIndex = os.path.splitext(self.FILENAME)
        print "%s - %s" % (fileIndex[1:],self.file.read(self.INDEXLEN))
        return fileIndex[1:] == self.file.read(self.INDEXLEN)


class ValidationError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class DatabaseError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

if __name__ == '__main__':
    myParams = ('knicks')
    db = Database('./db/',myParams)
    dbFiles = db.getDbFiles()
    for name in dbFiles:
        print name
        #currentFile = DbFile(name, db.currentDb)
        #currentFile.validateHeader()
