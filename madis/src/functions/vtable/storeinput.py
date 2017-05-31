# To change this template, choose Tools | Templates
# and open the template in the editor.
import os.path
import setpath
import sys
import imp
from lib.dsv import writer
import gzip
from lib.ziputils import ZipIter
import functions
from lib.vtoutgtable import vtoutpugtformat
import lib.inoutparsing
import os
import apsw
from collections import defaultdict
import json
from itertools import izip
import itertools
#import marshal as marshal
import cPickle
import pickle
import setpath
import vtbase
import functions
import struct
from array import array
#import marshal
import gc
import re
import zlib
import bz2
import cStringIO
import msgpack as marshal
### Classic stream iterator
registered=True
BLOCK_SIZE = 32768000


class NopVT(vtbase.VT):


    def VTiter(self, *args,**formatArgs):
        largs, dictargs = self.full_parse(args)
        where = None
        mode = 'row'

        if 'file' in dictargs:
            where=dictargs['file']
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No destination provided")
        if 'mode' in dictargs:
            mode = dictargs['mode']
        col = 0

        if 'cols' in dictargs:
            a = re.split(' |,| , |, | ,' , dictargs['cols'])
            column = [x for x in a if x != '']
        else:
            col = 1


        filename, ext=os.path.splitext(os.path.basename(where))
        fullpath=os.path.split(where)[0]
        fileIter=open(where, "rb")


        if mode == 'spac':
            import msgpack
            blocksize = struct.unpack('!i',fileIter.read(4))
            b = struct.unpack('!B',fileIter.read(1))
            schema = cPickle.load(fileIter)
            colnum = len(schema)

            blocknum = 0
            myvals = [[None] for _ in xrange(colnum)]
            yield schema
	    input = cStringIO.StringIO()
            while True:
		input.truncate(0)
                blocknum += 1

                d = 0
                ind = [0 for _ in xrange(colnum*3+1)]
                try:
                    blocksize = struct.unpack('!i',fileIter.read(4))
                except:
                    break
		input.write(fileIter.read(blocksize[0]))
                input.seek(0)
                block_kind = struct.unpack('!B',input.read(1))
                compression_bit = struct.unpack('!B',input.read(1))
                type = '!'+'i'*(colnum*3+1)
                ind = list(struct.unpack(type, input.read(4*(colnum*3+1))))

                d2 = [[] for _ in xrange(colnum)]
		col = 0
		
                column = marshal.loads(zlib.decompress(input.read(ind[col*3])))
		
                 
                if ind[col*3+2] == 0:
                            myvals[col] = column
                else:
                            myvals[col] = myvals[col] + column
                from itertools import imap
                if len(myvals[col])<256:
                            d2[col] = imap(list(myvals[col]).__getitem__, array('B', zlib.decompress(input.read(ind[col*3+1]))))
                else:
                            d2[col] = imap(list(myvals[col]).__getitem__, array('H', zlib.decompress(input.read(ind[col*3+1]))))


               
                for row in izip(*d2):
                    yield row


        if mode == 'sorteddictpercol':

#            if col:
#                print "lala"
#                gc.disable()
#                schema = marshal.load(fileIter)
#                colnum = len(schema)
#                cols = [[] for _ in xrange(colnum)]
#                yield schema
#                listptr = [array('H') for _ in xrange(colnum) ]
#                while True:
#                    try:
#                        row=0
#                        d = 0
#                        ind = struct.unpack('L'*(colnum+2), fileIter.read(8*(colnum+2)))
#                        for i in xrange(colnum):
#                            cols[i] = marshal.load(fileIter)
#                            listptr[i].fromfile(fileIter,ind[colnum+1])
#                        for row in xrange (ind[colnum+1]):
#                            tup = [0 for _ in xrange(colnum)]
#                            for col in xrange(colnum):
#                                tup[col] = cols[col][listptr[col][row]]
#                            yield tup
#                            tup = []
#
#                        listptr = [array('H') for _ in xrange(colnum) ]
#                    except:
#                        break
#                gc.enable()
#            elif len(column) == 1:
#                schema = marshal.load(fileIter)
#                colid = [x[0] for x in schema].index(column[0])
#                colnum = len(schema)
#                yield [schema[colid]]
#
#                while True:
#                    try:
#                        ind = struct.unpack('L'*(colnum+2), fileIter.read(8*(colnum+2)))
#                        listptr = array('H')
#                        next=ind[colnum]
#                        fileIter.seek(ind[colid])
#                        col = marshal.load(fileIter)
#                        listptr.fromfile(fileIter,ind[colnum+1])
#                        for c in listptr:
#                            yield(col[c],)
#                        fileIter.seek(next)
#                    except:
#                        break
#
#
#            else:
                import msgpack
                schema = msgpack.load(fileIter)
                colnum = len(schema)
                yield schema
                output = cStringIO.StringIO()
                blocknum = 0
                paxcols = {}
                while True:
                    try:
                        output.truncate(0)
                        blocksize = struct.unpack('i', fileIter.read(4))
#                        output.write(fileIter.read(blocksize[0]))
#                        output.seek(0)
                        ind = list(struct.unpack('L'*(colnum*2+1), fileIter.read(8*(colnum*2+1))))
                        d2 = [[] for _ in xrange(colnum)]
                        for c in xrange(colnum):
                            s = cPickle.loads(zlib.decompress(fileIter.read(ind[c*2])))
                            if (blocknum == 1 and c in paxcols) or (blocknum == 0 and len(s)>50*1.0*ind[colnum*2]/100):
                                d2[c] = s
                                if blocknum == 0:
                                    paxcols[c]=1
                            else:
                                if len(s)==1:
                                    d2[c] = [s[0] for _ in xrange(ind[colnum*2])]
                                elif len(s)<256:
                                    listptr = array('B')
                                    listptr.fromstring(zlib.decompress(fileIter.read(ind[c*2+1])))
                                    for lala in listptr:
                                        d2[c].append(s[lala])
                                else:
                                    listptr = array('H')
                                    listptr.fromstring(zlib.decompress(fileIter.read(ind[c*2+1])))
                                    for lala in listptr:
                                        d2[c].append(s[lala])
                        for row in izip(*d2):
                            yield row
                        blocknum = 1
                    except:
                        break


        if mode == 'dictperval':
            if col:
                gc.disable()
                schema = cPickle.load(fileIter)
                colnum = len(schema)
                cols = [[] for _ in xrange(colnum)]
                yield schema
                listptr = [array('H') for _ in xrange(colnum) ]
                while True:
                    try:
                        row=0
                        d = 0
                        ind = struct.unpack('L'*(colnum+3), fileIter.read(8*(colnum+3)))
                        for i in xrange(colnum):
                            cols[i] = cPickle.load(fileIter)
                        for i in xrange(colnum):
                            listptr[i].fromfile(fileIter,ind[colnum+2])

                        for row in xrange (ind[colnum+2]):
                            tup = [0 for _ in xrange(colnum)]
                            for col in xrange(colnum):
                                tup[col] = cols[col][listptr[col][row]]
                            yield tup
                            tup = []

                        listptr = [array('H') for _ in xrange(colnum) ]

                    except:
                        break
                gc.enable()
            elif len(column) == 1:
                schema = cPickle.load(fileIter)
                colid = [x[0] for x in schema].index(column[0])
                colnum = len(schema)
                yield [schema[colid]]
                while True:
                    try:
                        ind = struct.unpack('L'*(colnum+3), fileIter.read(8*(colnum+3)))
                        next=ind[colnum+1]
                        fileIter.seek(ind[colid])
                        col = cPickle.load(fileIter)
                        fileIter.seek(ind[colnum])
                        listptr = [array('H') for _ in xrange(colnum) ]
                        for i in xrange(colnum):
                            listptr[i].fromfile(fileIter,ind[colnum+2])
                        for c in listptr[colid]:
                            yield(col[c],)
                        fileIter.seek(next)
                    except:
                        break


            else:
                schema = marshal.load(fileIter)
                lcols = []
                for c in column:
                    lcols.append([x[0] for x in schema].index(c))

                colnum = len(schema)
                yield [schema[lcols[i]] for i in xrange(len(lcols))]
                while True:
                    row = 0
                    try:
                        d=0
                        ind = list(struct.unpack("<%dL" % ((colnum+1) * 2), fileIter.read(8*(colnum+1))))
                        next=ind[len(ind)-2]
                        d2 = [[] for _ in xrange(len(lcols))]
                        j = 0
                        for c in lcols:
                            fileIter.seek(ind[c*2])
                            d2[j] = marshal.load(fileIter)
                            j+=1
                        while True:
                            tup = []
                            for col in xrange(len(lcols)):
                                try:
                                    tup.append(d2[col][row])
                                except :
                                    d = 1
                                    break
                            if d == 1:
                                break
                            yield tup
                            tup = []
                            row+=1
                        fileIter.seek(next)
                    except:
                        break



        if mode == 'rcstreampax':
            if col:
                schema = marshal.load(fileIter)
                colnum = len(schema)
                ENDFILE = 0
                yield schema

                while True:
                    row=0
                    d = 0
                    ind = [0 for _ in xrange(colnum+2)]

                    if ENDFILE==1:
                        try:
                            marshal.load(fileIter)
                            ENDFILE=0
                        except EOFError:
                            break


                    for i in xrange(colnum+2):
                        ind[i] = struct.unpack('L',fileIter.read(8))
                    if ind[colnum+1][0] == 1:
                        ENDFILE = 1

                    d2 = [[] for _ in xrange(colnum)]

                    for col in xrange(colnum):
                        obj = fileIter.read(ind[col+1][0]-ind[col][0])
                        d2[col] = marshal.loads(zlib.decompress(obj))

                    while True:
                        tup = []
                        for col in xrange(colnum):
                            try:
                                tup.append(d2[col][row])
                            except :
                                d = 1
                                break
                        if d == 1:
                            break
                        yield tup
                        tup = []
                        row+=1


            elif len(column) == 1:
                schema = cPickle.load(fileIter)
                colid = [x[0] for x in schema].index(column[0])
                colnum = len(schema)
                yield [schema[colid]]
                while True:
                    try:
                        ind = list(struct.unpack("<%dL" % ((colnum+1) * 2), fileIter.read(8*(colnum+1))))
                        next=ind[len(ind)-2]
                        fileIter.seek(ind[colid*2])
                        d2 = cPickle.loads(zlib.decompress(fileIter.read(ind[colid*2+1]-ind[colid*2])))
                        #d2 = cPickle.load(fileIter)
                        for c in d2:
                            yield(c,)
                        fileIter.seek(next)
                    except:
                        break
            else:
                schema = marshal.load(fileIter)
                lcols = []
                for c in column:
                    lcols.append([x[0] for x in schema].index(c))

                colnum = len(schema)
                yield [schema[lcols[i]] for i in xrange(len(lcols))]
                while True:
                    row = 0
                    try:
                        d=0
                        ind = list(struct.unpack("<%dL" % ((colnum+1) * 2), fileIter.read(8*(colnum+1))))
                        next=ind[len(ind)-2]
                        d2 = [[] for _ in xrange(len(lcols))]
                        j = 0
                        fileIter.seek(ind[c*2])
                        d2[j] = marshal.load(fileIter)
                        j+=1
                        while True:
                            tup = []
                            for col in xrange(len(lcols)):
                                try:
                                    tup.append(d2[col][row])
                                except :
                                    d = 1
                                    break
                            if d == 1:
                                break
                            yield tup
                            tup = []
                            row+=1
                        fileIter.seek(next)
                    except:
                        break


        if mode == 'row':
            try:
                d2 =  cPickle.Unpickler(fileIter).load()
                yield d2

                while True:
                    try:
                        s = struct.unpack("i",fileIter.read(4))
                        for row in cPickle.loads(zlib.decompress(fileIter.read(s[0]))):
                            yield row
                    except:
                        break
            except EOFError,e:
                pass

        try:
            fileIter.close()
        except NameError:
            pass


def Source():
    return vtbase.VTGenerator(NopVT)

if not ('.' in __name__):
    """
    This is needed to be able to test the function, put it at the end of every
    new function you create
    """
    import sys
    import setpath
    from functions import *
    testfunction()
    if __name__ == "__main__":
        reload(sys)
        sys.setdefaultencoding('utf-8')
        import doctest
        doctest.testmod()


