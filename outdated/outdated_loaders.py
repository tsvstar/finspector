
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

