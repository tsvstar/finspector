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


def main():
    import db as mydb, debug
    """
    intermediary_saver = Saver('./!!my.~int~.db', period=-15)
    print "Scan filesystem"
    database={}
    scan_file( u"C:", database, intermediary_saver, verbose = True)
    print
    mydb.db_areas = {}
    mydb.save_real_db("./!!my.db",database,'main')
    """
    database = mydb.load_real_db("./!!my.db",{})

    db = mydb.FMTWrapper('./my_text.db', 'TEXT')
    #debug.Measure.measure_call_silent(db.fmt, db.save, database=database )
    debug.Measure.measure_call(db.fmt,  db.load, database={} )


    db = mydb.FMTWrapper('./my_bin1.db', 'BIN1')
    #debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )

    db = mydb.FMTWrapper('./my_bin2.db', 'BIN2')
    #debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )

    """
    db = mydb.FMTWrapper('./my_pick.db', 'PIK1')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )

    db = mydb.FMTWrapper('./my_pick.db', 'PIC1')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )


    db = mydb.FMTWrapper('./my_pick.db', 'PIKF')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )

    db = mydb.FMTWrapper('./my_pick.db', 'PICF')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )
    """
    db = mydb.FMTWrapper('./my_pick.db', 'UJSN')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )
    db = mydb.FMTWrapper('./my_pick.db', 'JSON')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )

    """
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
    """

    pass

if __name__ == '__main__':
    main()


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