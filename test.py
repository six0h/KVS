#!/usr/bin/python

from hashlib import sha256

myString = 'knicks'
hashedString = sha256(myString)
print hashedString.digest()