# coding=utf8

import sys, os, time, codecs


database = {}       #   [directory] ["dir"|"file"] [name]  = [0mtime, 1md5, 2size]
                    #   [directory] [name] = [0type, 1mtime, 2size, 3md5, 4hexstr_tstamp_snapshot?? ]       # type = File, Dir, Link

# filename +.asofDECTIMESTAMP       -- snapshot of state for exact asof day
# filename +.~intermediary          -- snapshot of state


import my.scandir as scandir

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
        print "Error for file %s (%s)" % ( fname.encode('cp866','ignore'), str(e).encode('cp866','ignore') )
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
            return database

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
    return database


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

def say( val ):
    if not isinstance(val,unicode):
        val = val.decode('utf-8')
    if sys.stdout.isatty():
        print val.encode('cp866','xmlcharrefreplace')
    else:
        sys.stdout.write( (val+u'\n').encode('utf-8','xmlcharrefreplace') )

def main():
    import my.db as mydb, my.debug as debug

    try:
        db = mydb.FMTWrapper()
        # a) Fastest way - is "msgpack" as a compiled-extension
        if db.getFormat('MSGp') and db.getFormat('MSGp').options.get('msgpack.ext',False):
            mydb.FMTWrapper.default_fmt = 'MSGp'
        else:
        # b) A little bit worse is UltraJSON compiled-extension
            import ujson
            # If UltraJSON exists - that is fastest way to load/save DB
            mydb.FMTWrapper.default_fmt = 'JSON'
    except Exception as e:
        # c) If no fast complied modules exists (PyPy) - use own text saver
        # It is faster other common solution - pickle/cPickle/json
        mydb.FMTWrapper.default_fmt = 'TEXT'
    print mydb.FMTWrapper.default_fmt

    if len(sys.argv)<2:
        print "No command given: update, find"
        exit(1)

    letter_list = [ 'C', 'G', 'H', 'K' ]
    letter_list = [ 'C','E' ]

    #mydb.FMTWrapper.default_fmt = 'TEXT'
    db = mydb.FMTWrapper('./!my_main.db')
    if sys.argv[1] in ['update','scan']:
        # COMMAND: rescan
        intermediary_saver = Saver('./!!my.~int~.db', period=-60)
        """
        print "Scan filesystem"
        database= scan_file( u"C:\\", {}, intermediary_saver, verbose = True)
        print
        print "Save"
        db.save( database )
	    """

        for letter in letter_list:
            db2 = mydb.FMTWrapper('./!my_%s.db'%letter)
            print "\nScan filesystem %s" %letter
            database= scan_file( u"%s:\\"%letter, {}, intermediary_saver, verbose = True)
            print "\nSave"
            db2.save( database )

    elif sys.argv[1]=='find':
        # COMMAND: simple lookup
        if len(sys.argv)<3:
            print "No pattern to find given"
            exit(1)

        database={}
        measure = debug.Measure()
        for letter in letter_list:
           print "Load %s:\\" %letter
           db = mydb.FMTWrapper('./!my_%s.db'%letter)
           db.load( database )
           cnt = reduce(lambda acc,x: acc+len(x), database.values(), 0)
        measure.tick('%d records loaded'%cnt)

        import re
        for pattern in sys.argv[2:]:
            print "Lookup '%s'" % pattern
            re_find = re.compile(pattern, flags=re.IGNORECASE)
            for dname,v1 in database.iteritems():
                basename = os.path.basename(dname)
                if re_find.search(basename):
                    say( dname )
                for fname in v1.keys():
                    if re_find.search(fname):
                        say( os.path.join(dname,fname) )

    elif sys.argv[1]=='md5':

        for letter in letter_list:
            print "Start processing %s" % letter
            db = mydb.FMTWrapper('./!my_%s.db'%letter)
            database = db.load()
            t0 = t1 = time.time()
            processed = processed_cycle = 0
            db.dirty = False
            for dname in database.keys():
                for fname in database[dname].keys():
                    v = database[dname][fname]
                    if v[0].lower()=='f' and len(v[3])!=22:
                        processed_cycle += v[2]
                        fullname = os.path.join(dname,fname)
                        sys.stdout.write( fullname.encode('cp866','ignore') +chr(13) )
                        v[3] = calculate_md5(fullname)
                        db.dirty = True
                        t2 = time.time()
                        if (t2-t1)>60:
                            mb = float(processed_cycle)/(1024*1024)
                            processed = float(processed) + mb
                            processed_cycle = 0
                            print "@tsv Save. Processed %.2fMB (Speed %.2fMB/s)" % ( mb, mb/(t2-t1) )
                            t1=t2
                            db.dirty = False
                            db.save()

            processed += processed_cycle
            t3 = time.time()
            print "@tsv Finished %s: (%.2fGb, speed=%.2fMb/s)" % ( letter, processed/1024, processed/(t3-t0))


    elif sys.argv[1]=='bench':
        # benchmark

        for letter in letter_list:
            print letter
            fname = './!my_%s.db'%letter
            db = mydb.FMTWrapper(fname)
            database = debug.Measure.measure_call_silent('',  db.load )
            cnt = reduce(lambda acc,x: acc+len(x), database.values(), 0)

            fmt = None  # keep format as is
            debug.Measure.measure_call_silent('%d records loaded\n'%cnt,  db.save, fname=fname+".bench", database=database, fmt=fmt )
    else:
        print "Unknown command: %s" % sys.argv[1]


    """
    ##print calculate_md5("C:\\MY\\proj\\zoid20140109.zip")
    """

    pass

if __name__ == '__main__':
    main()

