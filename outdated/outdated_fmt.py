


""" =============== OUTDATED SECTION  ============= """


"""
    Implementation: MY BINARY FORMAT v1
        JUST SAMPLE - not really remember long dir/filenames

    L = NUM_DIR_ENTRIES
      HHL22s99s = DirEntry( NUM_ENTIRES, len(dname), mtime, md5, dname
        H1s99sLQ22s = FileEntry( len(fname), ftype, fname, mtime, fsize, md5
# 0ftype, (1fname), 1mtime, 2fsize, 3md5, 4opt
                    s = struct.pack( 'H1s99sLL8s', len(fname1), v[0], fname1, v[1], v[2],v[3] )
"""
class _DBFMT_Pack1(_DBFMTMain):
    # description of class
    dbtype = 'BIN1'
    dbver  = '0001'
    require = ''    #''=os.open, otherwise = codecs.open

    def load( self, fd, database ):
        import struct
        file_size = os.fstat(fd).st_size
        pos = 0 #os.fdopen(fd).tell()
        f = os.read(fd, file_size-pos)

        offs1plus= struct.calcsize('HHL22s')
        offs2plus= struct.calcsize('H1s99sLQ22s')

        cntr=0
        try:
            offset = 0
            dlist_cnt = struct.unpack_from('L',f,offset)
            offset += 4
            dlist_cnt = dlist_cnt[0]
            print dlist_cnt
            while dlist_cnt>0:
                ln_dir, lndname, v1, v3 = struct.unpack_from( 'HHL8s', f, offset )
                dname = struct.unpack_from( '99s', f, offset+offs1plus )
                offset += offs1plus + 99 #24+99
                database[dname]={}
                while ln_dir>0:
                    vv = struct.unpack_from( 'H1s99sLL8s', f, offset )
                    offset+=offs2plus    #4+1+99+16+8
                    database[dname][vv[2]]=vv

                    ln_dir -=1
                    cntr+=1
                dlist_cnt-=1
        except:
            pass #raise
        print cntr, dlist_cnt
        return database

    def save( self, fd, database ):
        import struct
        print fd

        try:
            dirlst = sorted(database.keys())
            s1 = struct.pack( 'L', len(dirlst) )
            print len(dirlst)
            os.write( fd,  s1 )
            for dname in dirlst:
                cur = database.get( dname, [] )
                #dname = dname.encode('utf-8')
                v = cur['.']
                s = struct.pack( 'HHL8s99s', len(cur), len(dname), v[1], v[3], dname)#.encode('utf-8') )
                os.write( fd,  s )
                for fname in sorted(cur.keys()):
                    v = cur[fname]
                    #print type(fname)
                    #fname1= fname.encode('utf-8')
                    fname1=fname
                    s = struct.pack( 'H1s99sLQ8s', len(fname1), v[0].encode('ascii'), fname1, v[1], v[2],v[3].encode('utf-8') )
                    os.write( fd, s )
        finally:
            pass
            #os.close(fd)
        return database


"""
    Implementation: MY BINARY FORMAT v2
        TODO: load doesn't work

    L = NUM_OVERALL_ENTRIES
        cc22sLQH = Entry( ftype[!,D,F,L], _, md5, mtime, fsize, len(entry_name)
        plain_string contained concatenated entries in order of appearance without separators

# 0ftype, (1fname), 1mtime, 2fsize, 3md5, 4opt
"""
class _DBFMT_Pack2(_DBFMTMain):
    # description of class
    dbtype = 'BIN2'
    dbver  = '0001'
    require = ''    #''=os.open, otherwise = codecs.open

    def load( self, fd, database):
        """
        file_size = os.fstat(fd).st_size
        pos = 0 #os.fdopen(fd).tell()
        f = os.read(fd, file_size-pos)
        """
        #fd.reader.decode = codecs.utf_8_decode
        f = open(self.fname,'rb')
        #fd = os.open(self.fname, os.O_RDONLY)
        #fd.seek(self.offset)
        #f = fd.read()
        """
        file_size = os.fstat(fd).st_size
        print file_size
        pos = 0 #os.fdopen(fd).tell()
        f = os.read(fd, file_size-pos)
        print[f]
        f1 = os.read(fd, file_size-pos)
        print type(f)
        print len(bytearray(f))
        print len(f1)
        """

        offsplus= struct.calcsize('cc22sLQH')   # type, _, mtime, size, md5, namesize
        offset = self.offset+1
        try:
            ##print [ f[:4] ]
            print "%x"%offset
            f.seek(offset)
            r = f.read(4)
            print [r]
            entries = struct.unpack_from('L',r)[0]
            #entries = struct.unpack_from('L',f,offset)[0]
            print entries
            offset += 4
            startstr = offset + entries*offsplus

            stroffs = 0
            #strdecode = f[startstr:]#.decode('utf-8')
            print "%x"%startstr
            f.seek(startstr)
            print "%x"%f.tell()
            strdecode = f.read()

            f.seek(offset)

            while offset<startstr:
                s = f.read(offsplus)
                #ftyp, _, mtime, fsize, md5, lnname = struct.unpack_from( 'cc22sLQH', f, offset )
                ftyp, _, mtime, fsize, md5, lnname = struct.unpack( 'cc22sLQH', s )
                name = strdecode[stroffs:stroffs+lnname]
                stroffs+=lnname
                if ftyp=='!':
                    database[name] = {}
                    last = database[name]
                    #last['.']  = ['D', mtime,fsize,md5,'']
                else:
                    last[name] = [ftyp, mtime,fsize,md5,'']
                offset+=offsplus

        except:
            raise
        return database



    def save( self, fd, database ):
        import struct
        digitout=''

        offsplus= struct.calcsize('cc22sLQH')   # type, _, mtime, size, md5, namesize

        try:
        #with open( db_fname, 'wb' ) as f:
            # 0ftype, (1fname), 1mtime, 2fsize, 3md5, 4opt
            dirlst = sorted(database.keys())

            measure = debug.Measure(self)

            # calculate and store number of entries
            cntr = 0
            for dname in dirlst:
                cntr += 1+ len(database.get( dname, [] ))   # '!' record + entries
            os.write( fd, struct.pack('L',cntr) )
            measure.tick('count')

            sdirlst = []    # accumulate here sorted directory content (to correct order of names storage)
            # MAIN CYCLE
            for dname in dirlst:
                # store '!' entry (initialize prefix)
                cur = database.get( dname, [] )
                v = cur['.']
                os.write( fd, struct.pack( 'cc22sLQH', '!',' ', v[3],v[1],v[2], len(dname) ) )
                subdir = sorted(cur.keys())
                sdirlst.append(subdir)
                # store directory entries
                for fname in subdir:
                    v = cur[fname]
                    os.write( fd, struct.pack( 'cc22sLQH', v[0].encode('ascii'),' ', v[3],v[1],v[2], len(fname) ) )
            measure.tick('entries')
            os.write( fd, digitout )
            measure.tick('entries write')

            # store names
            for idx in xrange(0,len(dirlst)):
                os.write( fd, dirlst[idx] ) #.encode('utf-8')
                for fname in sdirlst[idx]:
                    os.write( fd, fname )   #.encode('utf-8')
            measure.tick('str write')
        finally:
            pass
        return database


class _DBFMT_PickBase(_DBFMTMain):
    # description of class
    dbtype = 'PIC_'
    dbver  = '0001'
    require = ''    #''=os.open, otherwise = codecs.open

    def load( self, fd, database):
        f = os.fdopen( fd )
        print type(self.pickle)
        db1 = self.pickle.load( f )
        #TODO: update
        return db1

    def save( self, fd, database):
        f = os.fdopen( fd, 'w')
        print type(self.pickle)
        database = self.pickle.dump( database, f )
        return database

class _DBFMT_Pickle(_DBFMT_PickBase):
    # description of class
    dbtype = 'PIK1'
    import pickle

class _DBFMT_cPickle(_DBFMT_PickBase):
    # description of class
    dbtype = 'PIC1'
    import cPickle as pickle


class _DBFMT_PickFastBase(_DBFMT_PickBase):
    # description of class
    dbtype = 'PIF_'
    dbver  = '0001'
    require = ''    #''=os.open, otherwise = codecs.open

    def save( self, fd, database):
        f = os.fdopen( fd,'w' )
        pickler = self.pickle.Pickler(f, self.pickle.HIGHEST_PROTOCOL)
        pickler.fast = 1
        pickler.dump(database)
        return database

class _DBFMT_PickleFast(_DBFMT_PickFastBase):
    # description of class
    dbtype = 'PIKF'
    import pickle
class _DBFMT_cPickleFast(_DBFMT_PickFastBase):
    # description of class
    dbtype = 'PICF'
    import cPickle as pickle



class _DBFMT_JSON(_DBFMTMain):
    # description of class
    dbtype = 'JSON'
    dbver  = '0001'
    require = 'utf-8'    #''=os.open, otherwise = codecs.open

    def load( self, f, database):
        import json
        db1=json.load(f)
        #TODO: update
        return db1

    def save( self, f, database ):
        import json
        json.dump(database, f)
        return database


class _DBFMT_TXTUnic(_DBFMT_TXT):
    # description of class
    dbtype = 'TXTU'
    dbver  = '0001'
    require = 'utf-8'    #''=os.open, otherwise = codecs.open

    # old style-save (slow)
    def save( self, f, database ):
        for dname in sorted(database.keys()):
            cur = database.get( dname, [] )
            v = cur['.']
            if not isinstance(dname,unicode):
                dname=dname.decode('utf-8')
            f.write( u"\n--DIR--|%s|%x|%s|%s\n" % (dname, v[1], v[3],v[4]) )
            for fname in sorted(cur.keys()):
                v = cur[fname]
                if fname!='.' and v[0]!='D':
                    if not isinstance(fname,unicode):
                        fname=fname.decode('utf-8')
                    f.write(u"%s|%s\t|%x|%d|%s|%s\n"%(v[0],fname,v[1],v[2],v[3],v[4]) )
        return database

        """
            #v = cur['.']
            if isinstance(dname,unicode):
                dname=dname.encode('utf-8')

            x = "\n--DIR--|%s|%x|%s|%s\n" % (dname, v[1], v[3],v[4])
            f.write( "\n--DIR--|%s|%x|%s|%s\n" % (dname, v[1], v[3],v[4]) )

                    try:
                        fname1=fname.decode('utf-8')
                    except:
                        print type(fname)
                        print fname.encode('cp866','ignore')
                        raise
        """
