# coding=utf8

# HEADER:
#   4s = tag
#   4s = fmt type
#   4x = version
#   4x = time
#   14s = db type
#       12345678901234
#       INTERMEDIARY
#       MAIN
#       SEGMENTED
#   2s =\r\n

import time, codecs, os
import debug

# 0ftype, (1fname), 1mtime, 2fsize, 3md5, 4opt

"""
    Base class for DB formats
"""
class _DBFMTMain(object):
    # description of class
    dbtype = '____'     # type of processor (%-4s)
    dbver  = '0001'     # hex value of version (%04x) - to adopt to changed internal values
    require = 'utf-8'   #''=os.open, otherwise = codecs.open

    def __init__(self):
        pass
    def load( self, f, database ):
        raise Exception("Unoverrided %s.load() method", type(self) )
    def save( self, f, database ):
        raise Exception("Unoverrided %s.save() method", type(self) )

"""
    Implementation: TEXT DATABASE
"""
class _DBFMT_TXT(_DBFMTMain):
    # description of class
    dbtype = 'TEXT'
    dbver  = '0001'
    require = 'utf-8'    #''=os.open, otherwise = codecs.open

    # instance value
    #lineno = 3

    def load( self, f, database ):
        try:
            dirname = None
            res = f.read().splitlines()
            for line in res:
                if not line:
                    continue
                line = line.split('|')
                if line[0][0]=='-':
                    dirname, mtime, md5, opt = line[1:]
                    val_lst = [u'D', int(mtime,16), 0, md5, opt ]
                    top,tail = split_dirpath(dirname)
                    if top in database:
                        database[top][tail] = val_lst
                    database[dirname] = { '.': list(val_lst) }
                else:
                    ftype, fname, mtime, fsize, md5, opt = line
                    database[dirname][fname[:-1]] = [ ftype, int(mtime,16), int(fsize), md5, opt ]
                self.lineno+=1
        except Exception as e:
            print "Broken DB file:\nIn %s at line %d\nError: %s\n" % ( 'db_fname', self.lineno, str(e) )
            return None
        return database

    def save( self, f, database ):
        for dname in sorted(database.keys()):
            cur = database.get( dname, [] )
            v = cur['.']
            f.write( u"\n--DIR--|%s|%x|%s|%s\n" % (dname.decode('utf-8'), v[1], v[3],v[4]) )
            for fname in sorted(cur.keys()):
                v = cur[fname]
                if fname!='.' and v[0]!='D':
                    try:
                        fname1=fname.decode('utf-8')
                    except:
                        print type(fname)
                        print fname.encode('cp866','ignore')
                        raise
                    f.write(u"%s|%s\t|%x|%d|%s|%s\n"%(v[0],fname1,v[1],v[2],v[3],v[4]) )
        return database


"""
    Wrapper: wrap all others formats processor (this class should be used to load/save DB)
            - auto detect format on load
            - write in requested format
            - process common auxilary data
    TODO: maybe include/exclude - just two lines with values separated with |
"""
class FMTWrapper(object):
    # STATIC MEMBERS
    formats = {}        # formats[dbfmt][ver]   -- autodetect
    save_formats = {}   # saveformats[dbfmt] = last_ver_of_db

    DB_HEADER_TAG = chr(7)+"@FI"
    DB_FMT_LINE = DB_HEADER_TAG+"FMT VER TIME12345678901234\r\n"

    # INSTANCE MEMBERS
    fmt = None
    ver = None
    dbtime = time.time()
    dbtype = '12345678901234'

    fname = ""
    database = None       # database[dirname][fname] = [ 0ftype, 1mtime, 2fsize, 3md5, 4opt ]

    def __init__( self, fname = '', fmt='TEXT'):
        if not self.formats:
            self.formats, self.save_formats = self.initFormats()
        self.fname = fname
        self.fmt = fmt
        self.areas = {}              # db_areas[name]
        self.areas_exclude = {}      # db_areas[name]
        self.database = {}

    @staticmethod
    def initFormats():
        for n,obj in globals().iteritems():
            try:
                #print issubclass(obj,_DBFMTMain), n, obj.dbtype
                if issubclass(obj,_DBFMTMain):
                    tgt = FMTWrapper.formats.setdefault(obj.dbtype, {} )
                    if obj.dbver in tgt:
                        print("Collision for format '%s:%s': %s and %s" % (obj.dbtype,obj.dbver, type(tgt[obj.dbver]), type(obj)) )
                        raise Exception("Collision for format '%s:%s': %s and %s" % (obj.dbtype,obj.dbver, type(tgt[obj.dbver]), type(obj)) )
                    tgt[obj.dbver] = obj
            except Exception as e:
                pass

        print FMTWrapper.formats
        for n, v1 in FMTWrapper.formats.iteritems():
            m = max(v1.keys())
            FMTWrapper.save_formats[n] = FMTWrapper.formats[n][m]
        return FMTWrapper.formats, FMTWrapper.save_formats

    def load( self, fname = None, database = None ):
        f = None
        if fname is not None:
            self.fname = fname
        if database is None:
            database = self.database

        try:
            f = codecs.open(self.fname,'rb','utf-8')

            # Load header
            lineno = 1
            res = f.read(len(self.DB_FMT_LINE))
            print len(self.DB_FMT_LINE)
            if res[:4]!=self.DB_HEADER_TAG:
                raise Exception("Wrong TAG: This is not a FInspector DB")
            self.fmt, self.ver, self.dbtime, self.dbtype = res[4:8], res[8:12], int(res[12:16],16), res[16:16+14]
            if self.fmt not in self.formats:
                raise Exception("Unknown DB Format: %s"%self.fmt)
            if self.ver not in self.formats[self.fmt]:
                raise Exception("Unsupported DB Ver: %s.%s"%(self.fmt,self.ver))
            lineno+=1


            offset = len(res)

            def emptydec(*kw,**kww):
                    #print kw
                    #print kww
                    return kw[0],len(kw[1])
            #f.encoding=None
            store = f.reader.decode
            f.reader.decode = emptydec
            lines = ''
            while lines.find('\n\n')<0:
                l=f.read(256)
                print "!!"
                if not l:
                    break
                lines+=l

            # Load areas of responsibility
            self.areas, self.areas_exclude = {}, {}
            idx = 0
            while True:
              try:
                stop = lines.find('\n',idx)+1
                s = lines[idx:stop]
                idx = stop

                #s = f.readline()
                print "!!",s
                offset += len(s)
                if not s.strip():
                    break
                if s[0]=='+':
                    self.areas[s[1:]]=1
                elif s[0]=='-':
                    self.areas_exclude[s[1:]]=1
                else:
                    raise Exception( "Unknown area type at line %d: %s" % (lineno,s) )
              finally:
                lineno+=1

            f.reader.decode = store
            f.seek( offset )        # fix offset after readline()

            # Find corresponend processor and call it
            obj = self.formats[self.fmt][self.ver]()
            print "Do load %s" %type(obj)
            obj.lineno = lineno
            obj.offset = offset
            if obj.require=='':
                rv = obj.load( f.fileno(), database )
            elif obj.require=='utf-8':
                rv = obj.load( f, database )
            else:
                with open(fname,'rb',obj.require) as f1:
                    f1.seek( f.tell() )
                    rv = obj.load( f1, database)

            # TODO!! Postprocess - clean out not matched to areas/areas_exclude
            ln=0
            for v1 in rv.itervalues():
                ln+=len(v1)
            print "Loaded %s records" % ln


            return rv

        finally:
            if f:
                f.close()


    def save( self, fname=None, database=None, dbtype='MAIN', fmt = None ):
        if fname is not None:
            self.fname = fname
        if database is None:
            database = self.database
        if fmt is not None:
            self.fmt = fmt
        if self.fmt not in self.save_formats:
            raise Exception("Fail to save: don't know how to write '%s' format"%self.fmt)
        with codecs.open( self.fname, 'wb', 'utf-8' ) as f:
            f.write("%4s%-4s%-4s%04x%-14s\r\n" % ( FMTWrapper.DB_HEADER_TAG, self.fmt, self.save_formats[self.fmt].dbver, time.time(),dbtype[:14] ) )
            for k in self.areas.keys():
                f.write("+%s\n"%k)
            for k in self.areas_exclude.keys():
                f.write("-%s\n"%k)
            f.write('\n')

            # TODO: Preprocess! Exclude not matched to areas/exclude_areas

            obj = self.save_formats[self.fmt]()
            print "Do save %s" %type(obj)
            if obj.require == 'utf-8':
                return obj.save( f, database )
            elif obj.require == '':
                f.flush()
                return obj.save( f.fileno(), database )

        with codecs.open( self.fname, 'ab', obj.require ) as f:
            return obj.save( f, database )


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
                s = struct.pack( 'HHL8s99s', len(cur), len(dname), v[1], v[3], dname.encode('utf-8') )
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
        file_size = os.fstat(fd).st_size
        pos = 0 #os.fdopen(fd).tell()
        f = os.read(fd, file_size-pos)

        offsplus= struct.calcsize('cc22sLQH')   # type, _, mtime, size, md5, namesize
        offset = 0
        try:
            entries = struct.unpack_from('L',f,offset)[0]
            offset = 8
            startstr = offset + entries*offsplus
            print entries

            stroffs = 0
            strdecode = f[startstr:].decode('utf-8')

            while offset<stroffs:
                ftyp, _, mtime, fsize, md5, lnname = struct.unpack_from( 'cc22sLQH', f, offset )
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



class _DBFMT_ULTRAJSON(_DBFMTMain):
    # description of class
    dbtype = 'UJSN'
    dbver  = '0001'
    require = 'utf-8'    #''=os.open, otherwise = codecs.open

    def load( self, f, database):
        import ujson as json
        db1=json.load(f)
        #TODO: update
        return db1

    def save( self, fd, database ):
        import ujson as json
        json.dump(database, fd)
        return database

class SEGMENTED_CLASS(object):
    segments = [ ]      # list of databases

    # def __iter__ - iter through all databases
    # setter
    # getter

""" =============== OUTDATED SECTION  ============= """

DB_HEADER_TAG = "tsv@f_inspector"

def load_real_db( db_fname, database ):
    #with codecs.open( db_fname, 'rb', 'utf-8' ) as f:
    #    print f.read().encode('utf-8')
    with codecs.open( db_fname, 'rb', 'utf-8' ) as f:
        try:
            lineno=1
            headerline = f.readline()
            header= headerline.split('|')
            if len(header)!=4 or header[0]!=DB_HEADER_TAG:
                raise Exception("Incorrect header")
            line = f.readline()
            lineno+=2

            tell = len(headerline)+len(line)
            f.seek(tell)

            import debug
            measure = debug.Measure(load_real_db)
            dirname = None
            res = f.read().encode('utf-8').splitlines()
            measure.tick('read+splitlines')
            for line in res:
                if not line:
                    continue
                line = line.split('|')
                if line[0][0]=='-':
                    dirname, mtime, md5, opt = line[1:]
                    val_lst = [u'D', int(mtime,16), 0, md5, opt ]
                    top,tail = split_dirpath(dirname)
                    if top in database:
                        database[top][tail] = val_lst
                    database[dirname] = { '.': list(val_lst) }
                else:
                    ftype, fname, mtime, fsize, md5, opt = line
                    database[dirname][fname[:-1]] = [ ftype, int(mtime,16), int(fsize), md5, opt ]
                lineno+=1
            measure.tick('parse')
        except Exception as e:
            print "Broken DB file:\nIn %s at line %d\nError: %s\n" % ( db_fname, lineno, str(e) )
            raise
            return None
    return database


def save_real_db( db_fname, database, db_type ):
    with codecs.open( db_fname, 'wb', 'utf-8' ) as f:
        f.write("%s|%s|%d|%s\n\n" % (DB_HEADER_TAG,db_type, time.time(),db_areas.get(db_fname) ) )
        for dname in sorted(database.keys()):
            cur = database.get( dname, [] )
            v = cur['.']
            f.write( u"\n--DIR--|%s|%x|%s|%s\n" % (dname, v[1], v[3],v[4]) )
            for fname in sorted(cur.keys()):
                v = cur[fname]
                if fname!='.' and v[0]!='D':
                    f.write(u"%s|%s\t|%x|%d|%s|%s\n"%(v[0],fname,v[1],v[2],v[3],v[4]) )


import struct
def save_real_db_pack( db_fname, database, db_type ):
    import os
    fd = os.open(db_fname,os.O_WRONLY|os.O_CREAT|os.O_BINARY)

    try:
    #with open( db_fname, 'wb' ) as f:
        # 0ftype, 1fname, 2mtime, 3fsize, 4md5, 5opt
        dirlst = sorted(database.keys())
        s1 = struct.pack( 'L', len(dirlst) )
        print len(dirlst)
        os.write( fd,  s1 )
        for dname in dirlst:
            cur = database.get( dname, [] )
            dname = dname.encode('utf8')
            v = cur['.']
            s = struct.pack( 'HHL8s99s', len(cur), len(dname), v[1], v[3], dname )
            os.write( fd,  s )
            for fname in sorted(cur.keys()):
                v = cur[fname]
                fname1= fname.encode('utf8')
                s = struct.pack( 'H1s99sLL8s', len(fname1), v[0], fname1, v[1], v[2],v[3] )
                os.write( fd, s )

        """
        for dname in :
            cur = database.get( dname, [] )
            v = cur['.']
            s = struct.pack( 'ssLss', 'D', dname.encode('utf8'), v[1], v[3],v[4] )
            os.write( fd,  structpack )
            os.write( fd,  s )
            for fname in sorted(cur.keys()):
                v = cur[fname]
                if fname!='.' and v[0]!='D':
                    os.write( fd, struct.pack( 'ssLLss', v[0], fname.encode('utf8'), v[1], v[2], v[3],v[4] ) )
        """
    finally:
        os.close(fd)

def save_real_db_pack2( db_fname, database, db_type ):
    import os
    fd = os.open(db_fname,os.O_WRONLY|os.O_CREAT|os.O_BINARY)
    digitout=''

    offsplus= struct.calcsize('cc22sLQH')   # type, _, mtime, size, md5, namesize

    try:
    #with open( db_fname, 'wb' ) as f:
        # 0ftype, (1fname), 1mtime, 2fsize, 3md5, 4opt
        dirlst = sorted(database.keys())
        sdirlst = []
        cntr = 0
        for dname in dirlst:
            cntr += 1+ len(database.get( dname, [] ))
        os.write( fd, struct.pack('L',cntr) )

        for dname in dirlst:
            cur = database.get( dname, [] )
            ##dname = dname.encode('utf8')
            v = cur['.']
            os.write( fd, struct.pack( 'cc22sLQH', '!',' ', v[3],v[1],v[2], len(dname) ) )
            #strout.append( dname )
            subdir = sorted(cur.keys())
            sdirlst.append(subdir)
            for fname in subdir:
                v = cur[fname]
                ##fname1= fname.encode('utf8')
                os.write( fd, struct.pack( 'cc22sLQH', v[0],' ', v[3],v[1],v[2], len(fname) ) )
                #strout.append( fname1 )

        os.write( fd, digitout )
        for idx in xrange(0,len(dirlst)):
            os.write( fd, dirlst[idx].encode('utf-8') )
            for fname in sdirlst[idx]:
                os.write( fd, fname.encode('utf-8') )

        """
        for dname in dirlst:
            cur = database.get( dname, [] )
            os.write(fd,dname)
            for fname in sorted(cur.keys()):
                os.write(fd,fname)

        for s in strout:
            os.write( fd, s )
        """

        """
        for dname in :
            cur = database.get( dname, [] )
            v = cur['.']
            s = struct.pack( 'ssLss', 'D', dname.encode('utf8'), v[1], v[3],v[4] )
            os.write( fd,  structpack )
            os.write( fd,  s )
            for fname in sorted(cur.keys()):
                v = cur[fname]
                if fname!='.' and v[0]!='D':
                    os.write( fd, struct.pack( 'ssLLss', v[0], fname.encode('utf8'), v[1], v[2], v[3],v[4] ) )
        """
    finally:
        os.close(fd)





def load_real_db_pack2( db_fname, database):
    import os
    with open(db_fname,'rb') as f:
        f = f.read()

    db2 ={}
    offsplus= struct.calcsize('cc22sLQH')   # type, _, mtime, size, md5, namesize
    try:
        entries = struct.unpack_from('L',f,offset)
        offset = 8
        startstr = offset + entries*offsplus

        stroffs = 0
        strdecode = f[startstr:].decode('utf-8')

        while offset<stroffs:
            ftyp, _, mtime, fsize, md5, lnname = struct.unpack_from( 'cc22sLQH', f, offset )
            name = strdecode[stroffs:stroffs+lnname]
            stroffs+=lnname
            if ftyp=='!':
                db2[name] = {}
                last = db2[name]
            # 0ftype, (1fname), 1mtime, 2fsize, 3md5, 4opt
                #last['.']  = ['D', mtime,fsize,md5,'']
            else:
                last[name] = [ftyp, mtime,fsize,md5,'']
            offset+=offsplus

    except:
        raise
    return db2


# PURPOSE: same as os.path.split(), but correctly process if directory names
def split_dirpath( dirname ):
    a = filter(len, dirname.split('\\') )
    if len(a)==1:
        return a[0], ''
    elif len(a)==2:
        return a
    top, tail = '\\'.join(a[:-1]), a[-1]
    return top, tail
