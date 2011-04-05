# originally from: http://code.activestate.com/recipes/84317/

from threading import *

class Future(object):
    def __init__(self):
        # Constructor
        self.__done=0
        self.__result=None
        self.__status='working'

        self.__C=Condition()   # Notify on this Condition when result is ready

    def __repr__(self):
        return '<Future at '+hex(id(self))+':'+self.__status+'>'

    def __call__(self):
        self.__C.acquire()
        while self.__done==0:
            self.__C.wait()
        self.__C.release()
        # We deepcopy __result to prevent accidental tampering with it.
        return self.__result

    def set(self, v):
        # Run the actual function, and let us housekeep around it
        self.__C.acquire()
        self.__result=v
        self.__done=1
        self.__status=`self.__result`
        self.__C.notify()
        self.__C.release()
