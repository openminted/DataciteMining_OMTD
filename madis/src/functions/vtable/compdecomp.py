import sys
import functions
import vtbase
import sys
import functions
import json
import datetime
import time
from madcomp import Compression , Decompression

registered=True


class CompBenchmark(vtbase.VT):

    def VTiter(self, *parsedArgs, **envars):
        largs, dictargs = self.full_parse(parsedArgs)
        # compressors vars
        compressor = []
        compressor_iter = []
        compresor_level_max = 0
        compressor_level = []
        compressor_ratio = []
        compressor_bandwidth = []
        data_size = 0

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No query argument ")
        query = dictargs['query']
        cur = envars['db'].cursor()
        q = cur.execute(query, parse=False)
        schema = list(cur.getdescriptionsafe())

        decompress = Decompression()
        compressor.append(Compression(schema))
        compresor_level_max = compressor[-1].getmaxlevel()
        compressor_iter.append(compressor[-1].compress())
        compressor_iter[-1].send(None)  # send gia initiation tou iterator
        compressor_level.append(0)
        compressor_iter[-1].send({'level': compressor_level[-1]})
        compressor_ratio.append(0.0)
        compressor_bandwidth.append(0)
        for i in xrange(1, compresor_level_max+1):
            compressor.append(Compression(schema))
            compressor_iter.append(compressor[-1].compress())
            compressor_iter[-1].send(None)  # send gia initiation tou iterator
            compressor_level.append(i)
            compressor_iter[-1].send({'level': compressor_level[-1]})
            compressor_ratio.append(0.0)
            compressor_bandwidth.append(0.0)

        counter = 0
        finished = False
        print 'first block:'
        for row in q:
            json_dumps = json.dumps(row, separators=(',', ':'), ensure_ascii=True, check_circular=False)
            data_size += len(json_dumps)
            for i in xrange(0, compresor_level_max+1):
                time_check_before = datetime.datetime.now()
                ret = compressor_iter[i].send(row)
                time_difference = datetime.datetime.now() - time_check_before
                if ret is not None:
                    compressor_ratio[i] = float(float(data_size)/len(ret))
                    compressor_bandwidth[i] = float(data_size)/time_difference.total_seconds()/1024.0/1024.0
                    print i, ': compressor ratio: ', compressor_ratio[i], ' compressor_bandwidt: ', compressor_bandwidth[i], 'MB/s'
                    if i == compresor_level_max/2:
                        for row in decompress.decompressblock(ret):
                                yield row
                    compressor_iter[i].send({'level': compressor_level[i]})
                    if i == compresor_level_max:
                        finished = True
                        data_size = 0
                        counter += 1
                        print 'second block:'
            # if ret is not None:
            #     for row in decompress.decompressblock(ret):
            #         yield row
            # if counter == 2:
            #     break
        ret = compressor_iter[compresor_level_max/2].send(None)
        for row in decompress.decompressblock(ret):
            yield row


class CompDecomp(vtbase.VT):
    def VTiter(self, *parsedArgs, **envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No query argument ")
        query = dictargs['query']
        cur = envars['db'].cursor()
        q = cur.execute(query, parse=False)
        schema = list(cur.getdescriptionsafe())

        print schema
        compress = Compression(schema)
        decompress = Decompression()
        iter = compress.compress()
        iter.send(None)  # send gia initiation tou iterator
        level = 0
        iter.send({'level': level})
        total_comp_time = 0.0
        total_decomp_time = 0.0

        compressed_blocks = []

        collect_timecheck = time.time()
        for row in q:
            comp_timecheck = time.time()
            ret = iter.send(row)
            if ret is not None:
                #compressed_blocks.append(ret)
            #     comp_time = time.time() - comp_timecheck
            #     coll_time = time.time() - collect_timecheck
            #     total_comp_time += coll_time
            #     decomp_timecheck = time.time()
            #     for row in decompress.decompressblock(ret):
            #             yield row
            #     decomp_time = time.time() - decomp_timecheck
            #     total_decomp_time += decomp_time
            #     collect_timecheck = time.time()
            #     print >> sys.stderr, 'collect time', coll_time-comp_time, 'compress time', comp_time, 'decompress time', decomp_time
                iter.send({'level': level})
        comp_timecheck = time.time()
        ret = iter.send(None)
        if ret is not None:
            compressed_blocks.append(ret)
        comp_time = time.time() - comp_timecheck
        coll_time = time.time() - collect_timecheck
        total_comp_time += coll_time
        decomp_timecheck = time.time()
        # for row in decompress.decompressblock(ret):  # apostoli None otan teleiwnei o iterator gia na pareis to teleytaio block
        #     yield row
        decomp_time = time.time() - decomp_timecheck
        total_decomp_time += decomp_time
        print >> sys.stderr, 'collect time', coll_time
        # print >> sys.stderr, 'total collect-compress time', total_comp_time
        # print >> sys.stderr, 'total decompress time', total_decomp_time
        iter.close()
        yield '1'


class Comp(vtbase.VT):

    def VTiter(self, *parsedArgs, **envars):
        largs, dictargs = self.full_parse(parsedArgs)

        if 'query' not in dictargs:
            raise functions.OperatorError(__name__.rsplit('.')[-1], "No query argument ")
        query = dictargs['query']
        cur = envars['db'].cursor()
        q = cur.execute(query, parse=False)
        schema = list(cur.getdescriptionsafe())

        print schema
        compress = Compression(schema)
        decompress = Decompression()
        iter = compress.compress()
        iter.send(None)  # send gia initiation tou iterator
        level = 0
        iter.send({'level': level})

        for row in q:
            ret = iter.send(row)
            if ret is not None:
                for row in decompress.decompressblock(ret):
                        yield row
                print level
                iter.send({'level': level})
        for row in decompress.decompressblock(iter.send(None)):  # apostoli None otan teleiwnei o iterator gia na pareis to teleytaio block
            yield row
        iter.close()


def Source():
    return vtbase.VTGenerator(CompDecomp)

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




