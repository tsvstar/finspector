
        """
        with open('!!tmp','wb') as f:
            with mydb.GZIPRawStream(f,5) as f1:
                f1.write('AAA')
                f1.write('BBBBBB')
                f1.write('342432')

        with open('!!tmp','rb') as f:
            f1 = mydb.GZIPRawStream(f)
            print f1.read()
        exit(1)
        """

        """
        try:
            import gzip
            f=gzip.GzipFile(filename='!test', mode='wb',compresslevel=5)
            f.write('AAAAA')
            f.close()
            f=gzip.GzipFile(filename='!test', mode='rb',compresslevel=5)
            f.read()
            print "!!!"

            import zlib
            compresslevel = 5
            compress = zlib.compressobj(compresslevel,
                                             zlib.DEFLATED,
                                             -zlib.MAX_WBITS,
                                             zlib.DEF_MEM_LEVEL,
                                             0)
            vvv = compress.compress('AAAA')
            vvv += compress.compress('BBBB')
            vvv += compress.flush()
            #self.crc = zlib.crc32("") & 0xffffffffL
            #self.crc = zlib.crc32(data, self.crc) & 0xffffffffL
            print vvv

            decompress = zlib.decompressobj(-zlib.MAX_WBITS)
            v2=decompress.decompress(vvv)
            v2+=decompress.flush()
            print v2
            print "result^^^"

            import gzip,cStringIO
            f2 = cStringIO.StringIO()
            f=gzip.GzipFile(fileobj=f2, mode='wb',compresslevel=5)
            ##obj.save( f, database )
            f.write("AAAAA")
            vvvv = f2.getvalue()
            print vvvv
            f2.close()
            f2 = cStringIO.StringIO(vvvv)
            f3=gzip.GzipFile(fileobj=f2,mode='r')
            vvv = f3.read()
            exit(1)
        finally:
            pass
        """

