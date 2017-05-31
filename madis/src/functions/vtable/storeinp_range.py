import os.path
import sys
import functions
import os
from itertools import izip, repeat, imap
import cPickle
import cStringIO
import vtbase
import struct
import os
import gc
import re
import zlib
from array import array
import marshal
### Classic stream iterator
registered=True
BLOCK_SIZE = 200000000

class UnionAllSDC_f(vtbase.VT):



    def VTiter(self, *args,**formatArgs):
        import bz2
        import msgpack
        
        serializer = msgpack
        largs, dictargs = self.full_parse(args)
        where = None
        mode = 'row'
        input = cStringIO.StringIO()

        if 'file' in dictargs:
            where=dictargs['file']
        else:
            raise functions.OperatorError(__name__.rsplit('.')[-1],"No destination provided")
        col = 0

        if 'cols' in dictargs:
            a = re.split(' |,| , |, | ,' , dictargs['cols'])
            column = [x for x in a if x != '']
        else:
            col = 1
        start = 0
        end = sys.maxint-1
        if 'start' in dictargs:
            start = int(dictargs['start'])
        if 'end' in dictargs:
            end = int(dictargs['end'])

        fullpath = str(os.path.abspath(os.path.expandvars(os.path.expanduser(os.path.normcase(where)))))
        fileIterlist = []
        for x in xrange(start,end+1):
            try:
                fileIterlist.append(open(fullpath+"."+str(x), "rb"))
            except:
                break

        if fileIterlist == []:
            try:
                fileIterlist = [open(where, "rb")]
            except :
                raise  functions.OperatorError(__name__.rsplit('.')[-1],"No such file")

        for filenum,fileIter in enumerate(fileIterlist):
                blocksize = struct.unpack('!i',fileIter.read(4))
                b = struct.unpack('!B',fileIter.read(1))
                schema = cPickle.load(fileIter)
                colnum = len(schema)
                minr = "1997-09-17"
		maxr = "1997-11-17"
                retcols = [7]
                if filenum == 0:
                    yield ("c1",)
                    #yield ("pkey",)
                input = cStringIO.StringIO()
                while True:
                    input.truncate(0)
                    try:
                        blocksize = struct.unpack('!i', fileIter.read(4))
                    except:
                        break
              
                    if blocksize[0]:
                        input.write(fileIter.read(blocksize[0]))
                        input.seek(0)
                        b = struct.unpack('!B', input.read(1))
                        if b[0]:
                            decompression = struct.unpack('!B', input.read(1))
                            if decompression[0]:
                                decompress = zlib.decompress
                            else:
                                decompress = bz2.decompress
                                
                            type = '!'+'i'*(colnum*2+1)
                            ind = list(struct.unpack(type, input.read(4*(colnum*2+1))))
                            cols = [[] for i in range(len(retcols))]
                            indexes = []
                            c = 10
                            
                            input.seek(sum(ind[0:c*2])+1+1+4*(colnum*2+1))
                            s = serializer.loads(decompress(input.read(ind[c*2])))
				
                            def binarySearchmi(alist, item):
                                first = 0
                                last = len(alist)-1
                                found = False
                                midpoint = (first + last)//2
                                while first<=last and not found:
                                    midpoint = (first + last)//2
                                    if alist[midpoint] == item:
                                          found = True
                                    else:
                                        if item < alist[midpoint]:
                                            last = midpoint-1
                                        else:
                                            first = midpoint+1
				if found == False:
				    if alist[midpoint] < item:
					midpoint+=1 
                                return midpoint, found
			    def binarySearchma(alist, item):
                                first = 0
                                last = len(alist)-1
                                found = False
                                midpoint = (first + last)//2
                                while first<=last and not found:
                                    midpoint = (first + last)//2
                                    if alist[midpoint] == item:
                                          found = True
                                    else:
                                        if item < alist[midpoint]:
                                            last = midpoint-1
                                        else:
                                            first = midpoint+1
				if found == False:
				    if alist[midpoint] > item: 
			                midpoint-=1
                                return midpoint, found
                            if (maxr >= s[0] and minr <= s[len(s)-1]):
                                    t1 = binarySearchmi(s,minr)
				    t2 = binarySearchma(s,maxr)
                                    if (len(s)>1 and ind[c*2+1]==0 and ind[colnum*2]>1):
                                        cols[0] = s
                                    else:

                                        if len(s)==1:
                                            cols[0] = repeat(s[0], ind[colnum*2])

                                        elif len(s)<256:
                                            listptr = array('B')
                                            listptr.fromstring(decompress(input.read(ind[c*2+1])))
                                            indices = [i for i, x in enumerate(listptr) if x >= t1[0] and x <= t2[0]]                                        
                                            for i in indices:
                                                indexes.append(i)
                                                cols[0].append(s[listptr[i]])
                                        else:
                                            listptr = array('H')
                                            listptr.fromstring(decompress(input.read(ind[c*2+1])) )
                                            indices = [i for i, x in enumerate(listptr) if t2[0]>= x >= t1[0]]
                                            j = 0
                                            for i in indices:
                                                indexes.append(i)
                                                j+=1
                                                cols[0].append(s[listptr[i]])

                                        # elif len(s)<256:
                                        #     cols = imap(s.__getitem__, array('B', decompress(input.read(ind[c*2+1]))))
                                        # else:
                                        #     cols = imap(s.__getitem__, array('H', decompress(input.read(ind[c*2+1]))))
                            else:
                                cols = [[]]*colnum
			    for i in cols[0]:
				yield [i]
                            if len(indexes)<0:
                                for c in retcols:
                                    if c != 10:
                                        input.seek(sum(ind[0:c*2])+1+1+4*(colnum*2+1))
                                        s = serializer.loads(decompress(input.read(ind[c*2])))
                                        if (len(s)>1 and ind[c*2+1]==0 and ind[colnum*2]>1):
                                            cols = s
                                        else:
                                            if len(s)==1:
                                                cols[c] = repeat(s[0], len(indexes))

                                            elif len(s)<256:
                                                listptr = array('B')
                                                listptr.fromstring(decompress(input.read(ind[c*2+1])))
                                                for i in indexes:
                                                    cols[c].append(s[listptr[i]])
                                            else:
                                                listptr = array('H')
                                                listptr.fromstring(decompress(input.read(ind[c*2+1])))
                                                for i in indexes:
                                                    cols[c].append(s[listptr[i]])

			                iterators = tuple(map(iter, cols))
			                ilen = len(cols)
			                res = [None] * ilen
			                
			                while True:
			                    ci = 0
			                    try:
			                        while ci < ilen:
			                            res[ci] = iterators[ci].next()
			                            ci += 1
			                        yield res
			                    except:
			                        break

                                cols = []
                        elif not b[0]:
                            schema = cPickle.load(fileIter)

        try:
            for fileObject in fileIterlist:
                fileObject.close()
        except NameError:
            pass


def Source():
    return vtbase.VTGenerator(UnionAllSDC_f)

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



