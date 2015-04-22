# coding=utf8

import sys, os, time, codecs


database = {}       #   [directory] ["dir"|"file"] [name]  = [0mtime, 1md5, 2size]
                    #   [directory] [name] = [0type, 1mtime, 2size, 3md5, 4hexstr_tstamp_snapshot?? ]       # type = File, Dir, Link

#DB FORMAT
#   HEADER
#       tsv@f_inspector|{main|intermediary|segmented}|asof_decimal_tstamp|area\n\n
#
#   LINES (ignore empty):
#       --DIR--|name|hex_hmtime|b64_md5??|optional_val
#       type|name\t|hex_mtime|dec_size\t|b64_md5|optional_val

# filename +.asofDECTIMESTAMP       -- snapshot of state for exact asof day
# filename +.~intermediary          -- snapshot of state


import scandir

# PURPOSE: same as os.path.split(), but correctly process if directory names
def split_dirpath( dirname ):
    a = filter(len, dirname.split('\\') )
    if len(a)==1:
        return a[0], ''
    elif len(a)==2:
        return a
    top, tail = '\\'.join(a[:-1]), a[-1]
    return top, tail

# PURPOSE: same as ljust(), but + cut center of path to make not larger then size
def fname_justify(fname, size=100):
    if fname.find('\\')<0:
        if len(fname)>size:
            return fname[4]+'..'+fname[-size+6:]
        return fname.ljust(size)

    top,tail = split_dirpath(fname)
    if (len(top)+len(tail)+1)<size:
        return ('\\'.join([top,tail])).ljust(size)
    elif (len(tail)>size-8):
        return ( top[:4]+"...\\"+tail[-size+8:] )

    return ( top[:(size-len(tail)-4)]+"...\\"+tail )


import hashlib, base64
def calculate_md5( fname, chunk_size = 512*1024 ):
    m = hashlib.md5()
    try:
        with open(fname, 'rb') as f:
            while True:
                s = f.read(chunk_size)
                if s:
                    m.update(s)
                if len(s)<chunk_size:
                    break
    except Exception as e:
        print "Error for file %s" % fname
    return base64.b64encode( m.digest())[:-2]


ignore_dirs_anyplace = ["\\Windows\\winsxs"]

def scan_file( dirname, database, intermediary_saver = None, verbose = False ):

    if dirname.find('\\')<0:
        dirname+='\\'

    if intermediary_saver:
        intermediary_saver.handle(database)
    if verbose:
        sys.stdout.write( fname_justify(dirname.encode('cp866','ignore')) + chr(13) )
        sys.stdout.flush()

    for i in ignore_dirs_anyplace:
        if dirname.find(i)>=0:
            print "\nIgnore dir:%s" % dirname.encode('cp866','ignore')
            return

    d = []
    if dirname not in database:
        s = os.lstat(dirname)
        cur_dir = database.setdefault( dirname, {'.': [u'D',s.st_mtime, 0, u'', u'']} )
    else:
        cur_dir = database[dirname]
    try:
        for l in list( scandir.scandir(dirname) ):
            s = l.stat(False)
            #print "%s\t%s\t%s\t%s" % (l.is_symlink(), s.st_size, s.st_mtime, l.name)
            if l.is_symlink():
                cur_dir[l.name] = [u'L', s.st_mtime, s.st_size, u'', u'']
            elif l.is_dir( False ):
                cur_dir[l.name] = [u'D', s.st_mtime, 0, u'', u'']
                database.setdefault( l.path, {'.': list(cur_dir[l.name]) } )
                d.append(l.path)
            else:
                cur_dir[l.name] = [u'F', s.st_mtime, s.st_size, u'md5', u'']
    except WindowsError as e:
        if verbose:
            print "\n"+str(e)

    for l in d:
        scan_file(l, database, intermediary_saver,verbose=verbose)


# calculate size and hashes:
#       if calcmd5 =False - calculate name+size hashes (draft hash for quickly detect moving, etc)
#       if calcmd5 =True  - calculate md5 (require completely refreshed MD5 of all files)
def calc_directories( database, calcmd5 = False ):

    # in reversed order to ensure that we are going from bottom
    for dname in reversed(database.keys()):
        curdir = database[dname]

        hashstr = u''
        size = 0
        for fname in sorted(curdir.keys()):
            v = curdir[fname]
            if fname=='.' or v[0]=='L':
                continue
            if calcmd5:
                if v[0]=='D':
                    hashstr += u"%s|%x|%x|%x\n" % (fname,v[1],v[2],v[5])    #DIR: name+mtime+size+md5_of_namesize_hash
                else:
                    hashstr += u"%s|%x|%x\n" % (fname,v[1],v[2])    #FILE: name+mtime+size
            else:
                hashstr += u"%s|%x|%x\n" % (fname,v[2],v[3])    #name+size+hash
            size += v[2]

        digest = hashlib.md5(hashstr).digest()
        if calcmd5:
            ar_safe_set( ar=curdir['.'], idx=5, value=digest, fill='' )         # database[dname][fname][5] for dir record is hashstr of md5_accumulated
                                                                                  # (to quickly find difference)
        else:
            ar_safe_set( ar=curdir['.'], idx=6, value=hashstr, fill='' )        # database[dname][fname][6] for dir record is name_sizes
            curdir['.'][3] = digest                                             # remember md5 of whole dir

        curdir['.'][2] = size                                                   # remember size
        top,tail = split_dirpath(dirname)                                       # copy to top
        if top in database:
            database[top][tail] = val_lst




DB_HEADER_TAG = "tsv@f_inspector"

def load_real_db( db_fname, database ):
    with codecs.open( db_fname, 'rb', 'utf-8' ) as f:
        try:
            lineno=1
            header = f.readline().split('|')
            if len(header)!=4 or header[0]!=DB_HEADER_TAG:
                raise Exception("Incorrect header")
            f.readline()
            lineno+=2

            measure_start()
            dirname = None
            res = f.read().splitlines()
            measure_stop()
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
            measure_stop()
        except Exception as e:
            print "Broken DB file:\nIn %s at line %d\nError: %s\n" % ( db_fname, lineno, str(e) )
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


def load_real_db_pack( db_fname, database):
    import os
    #fd = os.open(db_fname,os.O_RDONLY|os.O_BINARY)
    with open(db_fname,'rb') as f:
        f = f.read()

    db2 ={}
    offs1plus= struct.calcsize('HHL8s')
    offs2plus= struct.calcsize('H1s99sLL8s')

    print len(f)
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
            #print "%x> %s"%(offset+offs1plus, dname)
            offset += offs1plus + 99 #24+99
            db2[dname]={}
            while ln_dir>0:
                vv = struct.unpack_from( 'H1s99sLL8s', f, offset )
                offset+=offs2plus    #4+1+99+16+8
                db2[dname][vv[2]]=vv

                ln_dir -=1
                cntr+=1
            dlist_cnt-=1
    except:
        pass #raise
    print cntr, dlist_cnt



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


class Saver(object):
    def __init__( self, fname, period ):
        self.fname = fname
        self.period = period
        self.last_save = time.time()

    def handle( self, database ):
        t = time.time()
        if self.period>=0 and (t-self.last_save)>self.period:
            print "\n...intermediary DB saving..."
            self.last_save = t
            save_real_db( self.fname, database, "intermediary" )

db_areas ={}

def measure( call, *kw, **kww ):
    import time
    t1 = time.time()
    arg = ""
    if len(kw):  arg+= " %s"%repr(kw)
    if len(kww): arg+= " %s"%repr(kww)
    res = call(*kw, **kww)
    t2 = time.time()
    print "%s%s -- %.3f sec" % (call,arg, t2-t1)
    return res

def measure_silent( call, *kw, **kww ):
    import time
    t1 = time.time()
    arg = ""
    res = call(*kw, **kww)
    t2 = time.time()
    print "%s%s -- %.3f sec" % (call,arg, t2-t1)
    return res

def measure_start():
    global t3
    import time
    t3 = time.time()

def measure_stop():
    global t3
    import time
    t4 = time.time()
    print "%.3f"%(t4-t3)
    t3 = t4

def main():
    global db_areas
    #scan_file(u"C:\\Documents and Settings\\taranenko\\Downloads\\")

    database1 = measure( load_real_db, "./!!my.db", {} )
    measure_silent( save_real_db, "./!!my1.db", database1, None )
    measure_silent( save_real_db_pack2, './!!my1.dbc1',database1,None )
    db2 = measure_silent( load_real_db_pack, './!!my1.dbc',database1 )
    measure_silent( save_real_db, "./!!my1_test.db", db2, None )
    exit(1)

    if len(sys.argv)==3 and sys.argv[1]=='find':
        print "Load db"
        #database1 = load_real_db( "./!!my.db", {} )
        database1 = measure( load_real_db, "./!!my.db", {} )
        measure_silent( save_real_db, "./!!my1.db", database1, None )
        measure_silent( save_real_db_pack, './!!my1.dbc',database1,None )
        exit(1)

        import re
        re_find = re.compile(sys.argv[2], flags=re.IGNORECASE)
        for dname,v1 in database1.iteritems():
            basename = os.path.basename(dname)
            if re_find.search(basename):
                print dname
            for fname in v1.keys():
                if re_find.search(fname):
                    print os.path.join(dname,fname)
        exit(1)

    ##print calculate_md5("C:\\MY\\proj\\zoid20140109.zip")
    print "Load db"
    database1 = {}
    try:
        load_real_db("./!!my.~int~.db",database1)
    except:
        pass

    intermediary_saver = Saver('./!!my.~int~.db', period=-15)
    print "Scan filesystem"
    scan_file( u"C:", database, intermediary_saver, verbose = True)
    print

    print "Save"
    db_areas['./!!my.db'] = u"C:"
    save_real_db("./!!my.db",database,'main')

    pass

if __name__ == '__main__':
    main()


"""
pickle - works very slow
        import pickle
        import cPickle
        measure_start()
        with open('!pickle.dbc1','w') as f:
            pickler = cPickle.Pickler(f, cPickle.HIGHEST_PROTOCOL)
            pickler.fast = 1
            pickler.dump(database1)
            measure_stop()
        with open('!pickle.dbc1','r') as f:
            measure_silent( cPickle.load, f )
            measure_stop()

        with open('!pickle.dbc','w') as f:
            measure_silent( pickle.dump, database1, f )
        with open('!pickle.dbc','r') as f:
            measure_silent( pickle.load, f )
        exit(1)
"""

"""

def test1():
    v=['DD',10,11,'ttt','bbb']
    fname = 'asdfasdfasdfasdf'
    for i in range(0,239*1000):
        vvv = u"%s|%s\t|%x|%d|%s|%s\n"%(v[0],fname,v[1],v[2],v[3],v[4])

def testLoad1():
    v=['DD',10,11,'ttt','bbb']
    fname = 'asdfasdfasdfasdf'
    vvv = u"%s|%s\t|%x|%d|%s|%s\n"%(v[0],fname,v[1],v[2],v[3],v[4])
    for i in range(0,239*1000):
        ftype, fname, mtime, fsize, md5, opt = vvv.split('|')
        vvv1 = [ ftype, int(mtime,16), int(fsize), md5, opt ]


def testPack():
    v=['D',10,11,'ttt','bbb']
    fname = 'asdfasdfasdfasdf'
    import struct
#                    ftype, fname, mtime, fsize, md5, opt = line
    for i in range(0,239*1000):
        vvv = struct.pack("sslQss",v[0],fname,v[1],v[2],v[3],v[4] )

def testLoadPack():
    v=['DD',10,11,'ttt','bbb']
    fname = 'asdfasdfasdfasdf'
    import struct
    vvv = struct.pack("sslQss",v[0],fname,v[1],v[2],v[3],v[4] )
    for i in range(0,239*1000):
        vvv1 = struct.unpack("sslQss",vvv)

def TEST():
    measure( test1 )
    measure( testLoad1 )
    measure( testPack )
    measure( testLoadPack )

"""