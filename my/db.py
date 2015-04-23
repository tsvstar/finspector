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

""" TODO: Not sure that .load() needs to update database: intermediary is required for dir-scan only. and even more I actually not sure that it really required """

"""
        PYPY                                            PY2.7
load_real                                                                   load -- 1.336 sec
TEXT    save -- 2.047 sec       load -- 1.113 sec       save -- 2.716 sec   load -- 1.643 sec   [was rebuilt and now takes only ~0.95sec to save() for PYPY]
BIN1    save -- 1.656 sec       load -- 0.743 sec       save -- 3.755 sec   load -- 0.468 sec   [just demotest, will be much worse]
BIN2    save -- 3.013 sec       load -- 1.322 sec       save -- 5.356 sec   load -- 0.941 sec
JSON    save -- 7.347 sec       load -- 4.172 sec       save --11.550 sec   load -- 1.811 sec
USN     save -- absent          load -- absent          save -- 0.503 sec   load -- 1.200 sec
dict.copy 0.004
"""


import time, codecs, os
import debug


"""
============================================================
            Base class for DB formats
    Wrapper: wrap all others formats processor (this class should be used to load/save DB)
            - auto detect format on load
            - write in requested format
            - process common auxilary data
    TODO: * maybe include/exclude - just two lines with values separated with |
          * scan
============================================================
"""
class FMTWrapper(object):
    # STATIC MEMBERS
    formats = {}        # formats[dbfmt][ver]   -- autodetect
    save_formats = {}   # saveformats[dbfmt] = last_ver_of_db

    DB_HEADER_TAG = chr(7)+"@FI"
    DB_FMT_LINE = DB_HEADER_TAG+"FMT VER TIME12345678901234\r\n"

    #
    isDebug = False
    default_fmt = 'TEXT'

    # INSTANCE MEMBERS
    fmt = None
    ver = None
    dbtime = time.time()
    dbtype = '12345678901234'

    fname = ""
    database = None       # database[dirname][fname] = [ 0ftype, 1mtime, 2fsize, 3md5, 4opt ]

    def __init__( self, fname = '', fmt=None):
        if not self.formats:
            self.formats, self.save_formats = self.initFormats()
        self.fname = fname
        self.fmt = fmt if fmt is not None else self.default_fmt
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

        if FMTWrapper.isDebug:
            print FMTWrapper.formats
        for n, v1 in FMTWrapper.formats.iteritems():
            m = max(v1.keys())
            FMTWrapper.save_formats[n] = FMTWrapper.formats[n][m]
        return FMTWrapper.formats, FMTWrapper.save_formats

    def load( self, database = None, fname = None ):
        if self.isDebug: dbg = debug.Measure('load')
        f = None
        if fname is not None:
            self.fname = fname
        if database is None:
            database = self.database

        try:
            f = open(self.fname,'rb')   # codecs.open(self.fname,'rb','utf-8')

            # Load header
            lineno = 1
            res = f.read(len(self.DB_FMT_LINE))
            if res[:4]!=self.DB_HEADER_TAG:
                raise Exception("Wrong TAG: This is not a FInspector DB")
            self.fmt, self.ver, self.dbtime, self.dbtype = res[4:8], res[8:12], int(res[12:16],16), res[16:16+14]
            if self.fmt not in self.formats:
                raise Exception("Unknown DB Format: %s"%self.fmt)
            if self.ver not in self.formats[self.fmt]:
                raise Exception("Unsupported DB Ver: %s.%s"%(self.fmt,self.ver))
            lineno+=1

            offset = len(res)

            # Binary safe preload buffer to find empty line
            lines = ''
            while lines.find('\n\n')<0:
                l=f.read(256)
                if not l:
                    break
                lines+=l
            getline_vars = [0]
            def getline():
                stop = lines.find('\n',getline_vars[0])+1
                s = lines[getline_vars[0]:stop]
                getline_vars[0] = stop
                return s

            # Binary safe Load areas of responsibility
            self.areas, self.areas_exclude = {}, {}
            idx = 0
            while True:
              try:
                s = getline()
                offset += len(s)
                if not s.strip():
                    break
                elif s[0]=='+':
                    self.areas[s[1:].decode('utf-8')]=1
                elif s[0]=='-':
                    self.areas_exclude[s[1:].decode('utf-8')]=1
                else:
                    raise Exception( "Unknown area type at line %d: %s" % (lineno,s) )
              finally:
                lineno+=1

            f.seek( offset )        # fix offset after readline()

            if self.isDebug: dbg.tick('headers')

            # Find corresponend processor and call it
            obj = self.formats[self.fmt][self.ver]()
            if self.isDebug: print "Do load %s from %s" %( type(obj), self.fname)
            obj.lineno = lineno
            obj.offset = offset
            obj.fname = self.fname
            if obj.require=='':
                rv = obj.load( f, database )
            else:
                with open(fname,'rb',obj.require) as f1:
                    f1.seek( f.tell() )
                    rv = obj.load( f1, database)

            # TODO!! Postprocess - clean out not matched to areas/areas_exclude
            if self.isDebug:
                ln=0
                for v1 in rv.itervalues():
                    ln+=len(v1)
                print "Loaded %s records" % ln

            return rv

        finally:
            if f:
                f.close()


    def save( self, database=None, fname=None, dbtype='MAIN', fmt = None ):
        if fname is not None:
            self.fname = fname
        if database is None:
            database = self.database
        if fmt is not None:
            self.fmt = fmt
        if self.fmt not in self.save_formats:
            raise Exception("Fail to save: don't know how to write '%s' format"%self.fmt)

        with open( self.fname, 'wb' ) as f:
            f.write("%4s%-4s%-4s%04x%-14s\r\n" % ( FMTWrapper.DB_HEADER_TAG, self.fmt, self.save_formats[self.fmt].dbver, time.time(),dbtype[:14] ) )
            for k in self.areas.keys():
                if isinstance(k,unicode):
                    k = k.encode('utf-8')
                f.write("+%s\n"% k)
            for k in self.areas_exclude.keys():
                if isinstance(k,unicode):
                    k = k.encode('utf-8')
                f.write("-%s\n"%k)
            f.write('\n')

            # TODO: Preprocess! Exclude not matched to areas/exclude_areas

            obj = self.save_formats[self.fmt]()
            if self.isDebug: print "Do save %s to %s" % ( type(obj), self.fname )
            if obj.require == '':
                f.flush()
                return obj.save( f, database )

        with codecs.open( self.fname, 'ab', obj.require ) as f:
            return obj.save( f, database )


# 0ftype, (1fname), 1mtime, 2fsize, 3md5, 4opt

"""
============================================================
            Base class for DB formats
============================================================
"""
class _DBFMTMain(object):
    # description of class
    dbtype = '____'     # type of processor (%-4s)
    dbver  = '0001'     # hex value of version (%04x) - to adopt to changed internal values

    def __init__(self):
        pass
    def load( self, f, database ):
        raise Exception("Unoverrided %s.load() method", type(self) )
    def save( self, f, database ):
        raise Exception("Unoverrided %s.save() method", type(self) )

"""
    Implementation: TEXT DATABASE

#DB FORMAT
#   HEADER
#       tsv@f_inspector|{main|intermediary|segmented}|asof_decimal_tstamp|area\n\n
#
#   LINES (ignore empty):
#       --DIR--|name|hex_hmtime|b64_md5??|optional_val
#       type|name\t|hex_mtime|dec_size\t|b64_md5|optional_val

"""
class _DBFMT_TXT(_DBFMTMain):
    # description of class
    dbtype = 'TEXT'
    dbver  = '0001'
    require = ''                #''=os.open, otherwise = codecs.open

    # instance value
    #lineno = 3

    def load( self, f, database ):
        try:
            dirname = None

            ##measure = debug.Measure(self)
            res = f.read().decode('utf-8').splitlines()                     # produce unicode
            #res = f.read().encode('utf-8').splitlines()    # works faster but produce non-unicode
            ##measure.tick('read+split')
            lineno = self.lineno
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
            ##measure.tick('done')
        except Exception as e:
            print "Broken DB file:\nIn %s at line %d\nError: %s\n" % ( 'db_fname', lineno, str(e) )
            return None
        return database

    def save( self, f, database ):
        for dname in sorted(database.keys()):
            cur = database.get( dname, [] )
            _, mtime, _, md5, opt = cur['.']
            if isinstance(dname,unicode):
                dname=dname.encode('utf-8')
            if isinstance(md5,unicode):
                md5=md5.encode('utf-8')
            if isinstance(opt,unicode):
                opt=opt.encode('utf-8')
            try:
                f.write( "\n--DIR--|%s|%x|%s|%s\n" % (dname, mtime, md5, opt) )
            except:
                print [(dname, mtime, md5, opt)]
            for fname in sorted(cur.keys()):
                ftype, mtime, fsize, md5, opt = cur[fname]
                if isinstance(ftype,unicode):
                    ftype=ftype.encode('utf-8')
                if fname!='.' and ftype!='D':
                    if isinstance(fname,unicode):
                        fname=fname.encode('utf-8')
                    if isinstance(md5,unicode):
                        md5=md5.encode('utf-8')
                    if isinstance(opt,unicode):
                        opt=opt.encode('utf-8')
                    if isinstance(fname,unicode):
                        fname=fname.encode('utf-8')
                    f.write("%s|%s\t|%x|%d|%s|%s\n"%(ftype,fname,mtime,fsize,md5,opt) )
        return database



class _DBFMT_ULTRAJSON(_DBFMTMain):
    # description of class
    dbtype = 'UJSN'
    dbver  = '0001'
    require = 'utf-8'    #''=os.open, otherwise = codecs.open

    def load( self, f, database):
        try:
            import ujson as json
        except:
            import json
        db1=json.load(f)


        # update input database
        if not len(database):
            for k,v in db1.iteritems():
                database[k]=v
        else:
            for k,v in db1.iteritems():
                database.setdefault(k,{}).update(v)

        return db1

    def save( self, fd, database ):
        import ujson as json
        json.dump(database, fd)
        return database

"""============"""
class SEGMENTED_CLASS(object):
    segments = [ ]      # list of databases

    # def __iter__ - iter through all databases
    # setter
    # getter


""" AUXILARY FUNC"""

# PURPOSE: same as os.path.split(), but correctly process if directory names
def split_dirpath( dirname ):
    a = filter(len, dirname.split('\\') )
    if len(a)==1:
        return a[0], ''
    elif len(a)==2:
        return a
    top, tail = '\\'.join(a[:-1]), a[-1]
    return top, tail
