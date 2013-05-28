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
        for paths,dirs,files in os.walk(self.dbDir):
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
        self.UUIDLEN = 32
        self.UUIDOFFSET = 8
        self.INDEXLEN = 4
        self.INDEXOFFSET = 40 
        self.FIELDS = ('Magic','UUID','Index')
        self.FILENAME = fileName
        self.DB = db
        self.NOTFINISHED = 48
        self.fileOffset = 0
        self.blocks = []

        # self.getBlocks()

    def getBlocks(self):
        i = 0
        while self.FILESIZE > self.fileOffset:
            self.blocks.append(Block(self.fileOffset,self.file))
            block = self.blocks[i]
            blockLen = block.getTotalBlockLength()
            i = i + 1
            self.fileOffset = self.fileOffset + blockLen

class Block(object):
    def __init__(self,fileOffset,currentFile):
        self.VERSIONLEN = 4
        self.TIMESTAMPLEN = 8
        self.HASHLEN = 32
        self.LENGTHLEN = 4
        self.LENGTHOFFSET = self.VERSIONLEN + self.TIMESTAMPLEN + self.HASHLEN
        self.BLOCKORDER = ('version','timestamp','hash','length','json')
        self.currentFile = currentFile
        self.fileOffset = fileOffset
        self.JSONLEN = self.getJSONLength()
        self.BLOCKLENGTH = self.getTotalBlockLength()

    def getJSONLength(self):
        self.currentFile.seek(self.fileOffset + self.LENGTHOFFSET)
        bits = self.currentFile.read(self.LENGTHLEN)
        print bits,'file position',self.fileOffset
        return struct.unpack("<I", bits)[0]

    def getTotalBlockLength(self):
        return int(self.VERSIONLEN) + int(self.TIMESTAMPLEN) + int(self.HASHLEN) + int(self.LENGTHLEN) + int(self.JSONLEN)

class Validator(Database):
    def __init__(self,dir='./db/',*params):
        if(dir != None):
            self.dbDir = dir
        else:
            self.dbDir = '.'

        if(params != None):
            self.useDatabase(params[0])

    def validateFiles(self):
        for currentFile in self.files:
            self.validateFileHeader(currentFile)
            for block in currentFile.blocks:
                self.validateBlock(block)


    def validateFileHeader(self,currentFile):
        '''Begin validating all Fields'''
        while currentFile.fileOffset < 44:
            print "Current File: %s" % currentFile.FILENAME
            for f in currentFile.FIELDS:
                if getattr(self, 'validateFile%s' % f)(currentFile) == False:
                    raise ValidationError("Invalid header. The %s is broken." % (f))
                else:
                    print "Passed: %s" % f

    def validateFileMagic(self,currentFile):
        '''Validate the File Magic Number'''
        currentFile.file.seek(currentFile.MAGICOFFSET)
        currentFile.fileOffset = currentFile.fileOffset + currentFile.MAGICLEN
        return currentFile.file.read(currentFile.MAGICLEN) == 'caviar01'

    def validateFileUUID(self,currentFile):
        '''Validate the UUID'''
        currentFile.file.seek(currentFile.UUIDOFFSET)
        UUID = currentFile.file.read(currentFile.UUIDLEN)
        binUUID = sha256(currentFile.DB).digest()
        currentFile.fileOffset = currentFile.fileOffset + currentFile.UUIDLEN
        return UUID == binUUID

    def validateFileIndex(self,currentFile):
        '''Validate The File Index'''
        currentFile.file.seek(currentFile.INDEXOFFSET)
        fileName, fileExt = os.path.splitext(currentFile.FILENAME)
        fileExt = int(fileExt[1:])
        binFileIndex = struct.pack('<I', fileExt)
        currentFile.fileOffset = currentFile.fileOffset + currentFile.INDEXLEN
        return binFileIndex == currentFile.file.read(currentFile.INDEXLEN)

    def validateBlock(self,block):
        print "Validating:",block.VERSIONLEN

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
    myParams = ('sample')
    validator = Validator('./db/',myParams)
    dbFiles = validator.getDbFiles()
    validator.validateFiles()