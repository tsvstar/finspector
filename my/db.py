# coding=utf8

# HEADER:
#   4s 4s 4x 4x 14s 2s = (4s)tag, (4s)fmt_type, (4x)ver_num, (4x)timestamp, (14s)=db_type{'INTERMEDIARY','MAIN','SEGMENTED'}, {2s} = '\r\n'
# MAIN DATABASE FORMAT:
#   database[dname][fname] = [0ftype, 1mtime, 2fsize, 3md5{22sym-string}, 4opt]


""" TODO: Not sure that .load() needs to update database: intermediary is required for dir-scan only. and even more I actually not sure that it really required """

"""
        PYPY                                            PY2.7
load_real                                                                   load -- 1.336 sec
TEXT    save -- 2.047 sec       load -- 1.113 sec       save -- 2.716 sec   load -- 1.643 sec   [was rebuilt and now takes only ~0.95sec to save() for PYPY]
BIN1    save -- 1.656 sec       load -- 0.743 sec       save -- 3.755 sec   load -- 0.468 sec   [just demotest, will be much worse]
BIN2    save -- 3.013 sec       load -- 1.322 sec       save -- 5.356 sec   load -- 0.941 sec
JSON    save -- 7.347 sec       load -- 4.172 sec       save --11.550 sec   load -- 1.811 sec
UJSON   ----------               -------------          save -- 0.503 sec   load -- 1.200 sec
MSGPACK ----------               -------------          save -- 0.689 sec   load -- 0.807 sec
"""

import time, codecs, os, gzip, hashlib
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
    #DB_HEADER_TAG = "FI@T"
    DB_FMT_LINE = DB_HEADER_TAG+"FMT VER TIME12345678901234\r\n"

    #
    isDebug = False                     # print debug information
    default_fmt = 'TEXT'                # default format of database

    # INSTANCE MEMBERS
    fmt = None                  # this instance format
    ver = None                  # this instance format version was loaded
    dbtime = time.time()        #
    dbtype = '12345678901234'

    fname = ""              # name of file (or path)
    database = None         # database[dirname][fname] = [ 0ftype, 1mtime, 2fsize, 3md5, 4opt ]

    options = {}            # this will be copied to format class (control load/save nuances)


    """ METHODS """
    def __init__( self, fname = '', fmt=None):
        if not self.formats:
            self.formats, self.save_formats = self.initFormats()
        self.fname = fname
        self.fmt = fmt if fmt is not None else self.default_fmt
        self.areas = {}              # db_areas[name]
        self.areas_exclude = {}      # db_areas[name]
        self.database = {}
        self.options = FMTWrapper.options.copy()
        self.stream_format = {'gzip':GZIPStream, 'gzipraw':GZIPRawStream}

    @staticmethod
    def initFormats():
        for n,obj in globals().iteritems():
            try:
                #print issubclass(obj,_DBFMTMain), n, obj.dbtype
                if issubclass(obj,_DBFMTMain):
                    if obj.dbtype.startswith('__'):
                        continue
                    tgt = FMTWrapper.formats.setdefault(obj.dbtype, {} )
                    if obj.dbver in tgt:
                        print("Collision for format '%s:%s': %s and %s" % (obj.dbtype,obj.dbver, type(tgt[obj.dbver]), type(obj)) )
                        raise Exception("Collision for format '%s:%s': %s and %s" % (obj.dbtype,obj.dbver, type(tgt[obj.dbver]), type(obj)) )

                    if hasattr(obj,'init'):
                        result = obj.init()
                        if not result:
                            continue
                    tgt[obj.dbver] = obj
            except Exception as e:
                pass

        # Clean uninitialized formats
        for k in FMTWrapper.formats.keys():
            if not len(FMTWrapper.formats[k]):
                del FMTWrapper.formats[k]

        if FMTWrapper.isDebug:
            print FMTWrapper.formats
        for n, v1 in FMTWrapper.formats.iteritems():
            m = max(v1.keys())
            FMTWrapper.save_formats[n] = FMTWrapper.formats[n][m]
        return FMTWrapper.formats, FMTWrapper.save_formats

    def getFormat( self, fmt, ver = None ):
        if fmt not in self.formats:
            return None

        if ver is None:
            return self.save_formats[fmt]
        else:
            return self.formats[fmt].get(ver,None)

    # load all header for given file
    # PURPOSE:  a) fix headers without rebuilding body (example: if base file was renamed)
    #           b) read headers to quickly get info about database settings
    def loadHeaders( self, f, checkFunc = None ):
        self.lineno = 1
        res = f.read(len(self.DB_FMT_LINE))
        if res[:4]!=self.DB_HEADER_TAG:
            raise Exception("Wrong TAG: This is not a FInspector DB")
        self.fmt, self.ver, self.dbtime, self.dbtype = res[4:8], res[8:12], int(res[12:16],16), res[16:16+14].rstrip()
        if checkFunc is not None:
            checkFunc( self )

        # Binary safe preload buffer to find empty line
        lines = ''
        f.seek(0)
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
        s = getline()
        self.lineno += 1
        self.offset = len(s)
        #print "offset=",offset     #@tsv
        while True:
          try:
            s = getline()
            self.offset += len(s)
            if not s.strip():
                break
            elif s[0]=='+':
                self.areas[s[1:].decode('utf-8')]=1
            elif s[0]=='-':
                self.areas_exclude[s[1:].decode('utf-8')]=1
            elif s[0]=='#':
                # comment - skip it
                continue
            elif s[0]=='@':
                optname,optvalue = s[1:].decode('utf-8').split('=',1)
                self.options[ optname.strip() ] = optvalue.strip()
            else:
                raise Exception( "Unknown header type at line %d: %s" % (self.lineno,s) )
          finally:
             self.lineno+=1

        f.seek( self.offset )        # fix offset after readline()

    def saveHeaders( self, f ):
            f.write("%4s%-4s%-4s%04x%-14s\r\n" % ( FMTWrapper.DB_HEADER_TAG, self.fmt, self.ver, time.time(), self.dbtype ) )
            f.write( time.strftime("#ASOF: %d.%m.%y %H:%M\n", time.localtime(self.dbtime)) )
            for optname, optvalue in self.options.items():
                f.write("@%s=%s\n"%(optname.encode('utf-8'), optvalue.encode('utf-8')) )  #,'backslashreplace'
            for k in self.areas.keys():
                if isinstance(k,unicode):
                    k = k.encode('utf-8')
                f.write("+%s\n"% k)
            for k in self.areas_exclude.keys():
                if isinstance(k,unicode):
                    k = k.encode('utf-8')
                f.write("-%s\n"%k)
            f.write('\n')

    def load( self, database = None, fname = None ):
        if self.isDebug: dbg = debug.Measure('load')
        f = None
        if fname is not None:
            self.fname = fname
        if database is None:
            database = self.database

        try:
            f = open(self.fname,'rb')   # codecs.open(self.fname,'rb','utf-8')

            def checkHeader( self ):
                if self.getFormat( self.fmt ) is None:
                    raise Exception("Unknown DB Format: %s"%self.fmt)
                if self.getFormat( self.fmt, self.ver ) is None:
                    raise Exception("Unsupported DB Ver: %s.%s"%(self.fmt,self.ver))

            self.loadHeaders( f, checkHeader )

            if self.isDebug: dbg.tick('headers')

            # Find corresponend processor and call it
            obj = self.getFormat( self.fmt, self.ver )()
            if self.isDebug: print "Do load %s from %s" %( type(obj), self.fname)
            obj.lineno = self.lineno
            obj.offset = self.offset
            obj.fname = self.fname
            obj.options.update( self.options )
            sformat = self.options.get('compress','').split(':',1)[0]
            stream = self.stream_format.get( sformat, BaseStream )
            with stream(f) as f1:
                rv = obj.load( f1, database )

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
        if self.getFormat( self.fmt ) is None:
            raise Exception("Fail to save: don't know how to write '%s' format"%self.fmt)

        obj = self.getFormat( self.fmt )()
        self.ver = obj.dbver
        self.dbtype = dbtype[:14]
        self.dbtime = time.time()
        obj.options.update( self.options )

        fname_bak = self.fname+".bak"
        if os.path.exists(self.fname):
            if os.path.exists(fname_bak):
                os.unlink(fname_bak)
            os.rename(self.fname,fname_bak)

        with open( self.fname, 'wb' ) as f:
            self.saveHeaders( f )

            # TODO: Preprocess! Exclude not matched to areas/exclude_areas

            compress = self.options.get('compress','').split(':',1)
            if self.isDebug: print "Do save %s to %s" % ( type(obj), self.fname )
            clevel = int(compress[1]) if len(compress)>1 else 5
            if compress[0]!='':
                if compress[0] not in self.stream_format:
                    raise Exception("Fail to save: don't know '%s' compress "%compress)
                if self.isDebug: print "Compress as %s" % ( compress )
            stream = self.stream_format.get( compress[0], BaseStream )
            f.flush()

            with stream(f,compresslevel = clevel) as f1:
                rv = obj.save( f1, database )

        if os.path.exists(fname_bak):
            os.unlink(fname_bak)


"""
============================================================
            Stream classes
============================================================
"""

class BaseStream(object):
    def __init__( self, f, *kw,**kww ):
        self._fh = f
    def __getattr__( self, method, *kw,**kww):
        return getattr( self._fh, method )
    def close(self):
        pass
    def __enter__(self, *kw,**kww):
        return self
    def __exit__(self, *kw,**kww):
        self.close()

class UTF8Stream(BaseStream):
    def read( self, *kw,**kww ):
        rv = self._fh.read(*kw)
        if isinstance(rv,str):
            return rv.decode('utf-8')
        return rv
    def write( self, s ):
        if isinstance(s,unicode):
            s = s.encode('utf-8')
        return self._fh.write(s)

import zlib, struct

# Very simple class: doesn't support seek/tell, load whole file...
# But this make operations noticeable faster than GZIPStream
class GZIPRawStream(BaseStream):

    def __init__( self, f, compresslevel = 5, *kw,**kww ):
        self._fh = f
        self.offset = f.tell()
        self.crc = zlib.crc32("") & 0xffffffffL
        if f.mode[0]=='r':
            ln = self._read32u(f)
            crc = self._read32u(f)
            decompress = zlib.decompressobj(-zlib.MAX_WBITS)
            data = f.read(ln)
            self.crc = zlib.crc32(data, self.crc) & 0xffffffffL
            if self.crc!=crc:
                print "Wrong CRC: expect %x but got %x" % (crc, self.crc)
            self.content = decompress.decompress(data)
            self.content += decompress.flush()
            self.tell = 0
        else:
            self._write32u(f,0)
            self._write32u(f,0)
            self._len = 0
            self.compress = zlib.compressobj(compresslevel,
                                             zlib.DEFLATED,
                                             -zlib.MAX_WBITS,
                                             zlib.DEF_MEM_LEVEL,
                                             0)

    def close(self):
        if hasattr(self,'compress'):
            data = self.compress.flush()
            self._len+=len(data)
            self.crc = zlib.crc32(data, self.crc) & 0xffffffffL
            self._fh.write(data)
            self._fh.seek(self.offset)
            self._write32u(self._fh,self._len)
            self._write32u(self._fh,self.crc)

    def read(self,size=None):
        tell = self.tell
        if size is None:
            size = len(self.content)
        self.tell = self.tell+size
        self.tell = len(self.content) if self.tell>len(self.content) else self.tell
        return self.content[tell:self.tell]

    def write(self,s):
        data = self.compress.compress(s)
        self._fh.write(data)
        self.crc = zlib.crc32(data, self.crc) & 0xffffffffL
        self._len += len(data)

    def _write32u(self, f, val):
        f.write( struct.pack('L',val) )

    def _read32u(self, f):
        return struct.unpack( 'L', f.read(4) )[0]

class GZIPStream(BaseStream):

    def __init__( self, f, compresslevel = 5 ):
        self.gz=gzip.GzipFile(fileobj=f, compresslevel=compresslevel)

    def close(self):
        self.gz.close()

    def read(self,*kw,**kww):
        return self.gz.read(*kw,**kww)

    def write(self,*kw,**kww):
        return self.gz.write(*kw,**kww)


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
    options = {}

    def __init__(self):
        self.options = self.options.copy()
    def load( self, f, database ):
        raise Exception("Unoverrided %s.load() method", type(self) )
    def save( self, f, database ):
        raise Exception("Unoverrided %s.save() method", type(self) )

    @staticmethod
    def _update_db( database, db1 ):
        # update input database
        if not len(database):
            for k,v in db1.iteritems():
                database[k]=v
        else:
            for k,v in db1.iteritems():
                database.setdefault(k,{}).update(v)
        return database


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

    # instance value
    #lineno = 3

    def load( self, f, database ):
        try:
            dirname = None

            ##measure = debug.Measure(self)
            lineno = self.lineno
            res = f.read().decode('utf-8').splitlines()                     # produce unicode
            #res = f.read().encode('utf-8').splitlines()    # works faster but produce non-unicode
            ##measure.tick('read+split')
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
    dbtype = 'JSON'
    dbver  = '0001'

    @staticmethod
    def init():
        try:
            import ujson
            _DBFMT_ULTRAJSON.options['json.lib'] = 'ujson'   # fastest available library
        except ImportError:
            _DBFMT_ULTRAJSON.options['json.lib'] = 'json'    # fallback library
        return True

    def _get_lib( self ):
        try:
            if self.options.get('json.lib','ujson').lower() == 'ujson':
                import ujson as json
            else:
                import json
        except:
            import json
        return json

    def load( self, f, database):
        f = UTF8Stream(f)
        json = self._get_lib()
        db1=json.load(f)
        return self._update_db( database, db1 )

    def save( self, f, database ):
        f = UTF8Stream(f)
        json = self._get_lib()
        json.dump(database, f,ensure_ascii=False)
        return database


class _DBFMT_MSGPACK(_DBFMTMain):
    # description of class
    dbtype = 'MSGp'
    dbver  = '0001'

    @staticmethod
    def init():
        try:
            from msgpack import _unpacker, _packer
            module = True      # means "use c-extension"
        except ImportError as e:
            #print e
            try:
                import msgpack
                module  = False     # means "use pure python"
            except ImportError:
                module = None       # means "no msgpack module found"

        _DBFMT_MSGPACK.options['msgpack.ext'] = module
        print module
        return module is not None

    def load( self, f, database ):
        import msgpack
        db1 = msgpack.unpack( f )
        return self._update_db( database, db1 )

    def save( self, f, database ):
        import msgpack
        msgpack.pack( database, f )
        return database


"""============"""
class SegmentedDB(object):
    segments = {}       # list of databases

    # def __iter__ - iter through all databases
    # setter
    # getter
    def __init__( self, fname = None ):
        self._insensitive = lambda s:  s.replace('/','\\').rstrip('\\').upper()+'\\'
        #self._insensitive = lambda s: s

        self.segments = {}      # segments[dbname] = { '+':[include_dir], '-':[exclude_dir], 'db': FMTWrapper(if loaded),
                                #                         'md5_main': md5_of_db(name+size+mtime) to detect save, 'md5_md5': md5 of md5 )
        self.database = {}

        if fname is not None:
            self.load( fname )

    def load( self, fname ):
        self.fname = fname
        with open(fname,'rb') as f:
            lines = f.read().splitlines()
            last = None
            lineno = 0
            for l in lines:
                lineno += 1
                if lineno==1:
                    if not l.strip().startswith("#FILE_INSPECTOR_SEGMENTED_DB"):
                        raise Exception( "Not segmented format - tag is %s" % l )

                # skip comments
                if l.lstrip().startswith('#'):
                    continue

                # split the line
                dbname, path = (l.strip().split('\t',1) + [None] )[:2]   # cutoff comment, split to
                if dbname.strip():
                    sign = dbname[:1]
                    if sign not in ['-','+']:
                        print "Unknown sign '%s' at line %d" % (sign,lineno)
                        last = None
                        continue
                    if dbname[1:].strip():
                        last = dbname[1:]
                if path is None:
                    continue
                if not last:
                    print "Not defined database at line %d" % lineno
                    continue
                paths = filter( len, map( str.strip, path.split('|') ) )
                for p in paths:
                    self.segments.setdefault(last,{}).setdefault(sign,set()).add( self._insensitive(p) )

            print self.segments
            for dbname,v in self.segments.items():
                if '+' not in v:
                    print "There is no INCLUDE values for database '%s' defined" % dbname
                    del self.segments[dbname]
                v.setdefault('-',set())

            for dbname,v in self.segments.items():
                for dbname1,v1 in self.segments.items():
                    if dbname!=dbname1:

                        # check for direct collision
                        for p in v['+']:
                            if p in v1['+']:
                                print "Same value '%s' is defined both for '%s' and '%s'" % (p,dbname,dbname1)
                                exit()

                        # exclude other dir
                        for p in v['+']:
                            v1['-'].add(p)

            # compress:
            for dbname,v in self.segments.items():
                #   a) remove include values which are subdir of another records
                for p_include in list(v['+']):
                    for p_include2 in list(v['+']):
                        if p_include!=p_include2 and p_include.startswith(p_include2):
                            v['+'].remove(p_include)

                #   b) delete exclude items which are not started as
                for p_exclude in list(v['-']):
                    match = filter( lambda p_include: p_exclude.startswith(p_include), v['+'] )
                    if not match:
                        v['-'].remove(p_exclude)

            # transform to sorted list
            for dbname, v in self.segments.items():
                v['+']  = sorted(v['+'])
                v['-']  = sorted(v['-'])
                v['db'] = None
                v['md5_main'] = hashlib.md5()
                v['md5_md5']  = hashlib.md5()


    # detect to which database match
    def match( self, dname ):
        dname = self._insensitive( dname )
        for dbname,v in self.segments.iteritems():
            for p in v['+']:
                if dname.startswith(p):
                    break
            else:
                continue

            for p in v['-']:
                if dname.startswith(p):
                    break
            else:
                return dbname
        return None

    # load segments for given list of dir
    def load_segments( self, lst ):
        pass

    # save all loaded segments
    def save_segments( self ):
        pass

    # auxilary func
    def _calc_md5( self, dname, recursive = True ):
        pass



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
