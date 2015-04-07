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
                lineno+=1
        except Exception as e:
            print "Broken DB file:\nIn %s at line %d\nError: %s\n" % ( db_fname, lineno, str(e) )
            return False
    return True


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
    global db_areas
    #scan_file(u"C:\\Documents and Settings\\taranenko\\Downloads\\")

    print calculate_md5("C:\\MY\\proj\\zoid20140109.zip")
    print "Load db"
    database1 = {}
    load_real_db("./!!my.db",database1)

    intermediary_saver = Saver('./!!my.db', period=-15)
    print "Scan filesystem"
    scan_file( u"C:", database, intermediary_saver, verbose = True)
    print

    print "Save"
    db_areas['./!!my_full.db'] = u"C:"
    save_real_db("./!!my_full.db",database,'main')

    pass

if __name__ == '__main__':
    main()
