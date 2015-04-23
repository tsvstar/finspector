# coding=utf8

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
"""

    def copy( v ):
        return v.copy()
    database = debug.Measure.measure_call_silent('', copy, database )


    db = mydb.FMTWrapper('./my_text.db', 'TEXT')
    debug.Measure.measure_call_silent(db.fmt, db.save, database=database )
    debug.Measure.measure_call(db.fmt,  db.load, database={} )
    db = mydb.FMTWrapper('./my_ujsn.db', 'UJSN')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )


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


    db = mydb.FMTWrapper('./my_bin1.db', 'BIN1')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )

    db = mydb.FMTWrapper('./my_bin2.db', 'BIN2')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )


    db = mydb.FMTWrapper('./my_json.db', 'JSON')
    debug.Measure.measure_call_silent(db.fmt,  db.save, database=database )
    debug.Measure.measure_call(db.fmt, db.load, database={} )
"""

"""
    #database={}
    #debug.Measure.measure_call_silent( '', scan_file, u"C:", database, intermediary_saver, verbose = True)

    #mydb.db_areas = {}
    #debug.Measure.measure_call_silent( 'SAVE', mydb.save_real_db,"./!!my.db",database,'main')
    #(db.fmt,  db.load, database={} )
"""

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

        exit(1)

"""

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
