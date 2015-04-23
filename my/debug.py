# coding=utf8
import time

class Measure():
    name = ''

    def __init__( self, name='' ):
        self.t1 = time.time()
        self.name = name
        if name:
            print "Start %s"%name

    def tick( self, name ):
        t2 = time.time()
        if not name:
            name = 'tick'
        print "%s %s  = %.3fsec" % (self.name, name, t2-self.t1)
        self.t1 = t2

    @staticmethod
    def OpenContext( name ):
        self = Measure( name )
        return self

    @staticmethod
    def _measure_call( __prefix, call, arg, *kw, **kww ):
        t1 = time.time()
        res = call(*kw, **kww)
        t2 = time.time()
        print "%s%s%s -- %.3f sec" % (__prefix, call,arg, t2-t1)
        return res

    @staticmethod
    def measure_call( __prefix, call, *kw, **kww ):
        arg = ""
        if len(kw):  arg+= " %s"%repr(kw)
        if len(kww): arg+= " %s"%repr(kww)
        return Measure._measure_call( __prefix,call, arg, *kw, **kww )

    @staticmethod
    def measure_call_silent( __prefix, call, *kw, **kww ):
        return Measure._measure_call( __prefix, call, '', *kw, **kww )

    def __enter__( self, *kw, **kww):
        self.t1 = time.time()
        return self
    def __exit__( self, *kw, **kww):
        tick('end')

