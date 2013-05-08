"""Utility functions to log timing information for function calls

>>> logging.basicConfig()
>>> log.setLevel(1)
>>> def f(x):
...   z = 0
...   for i in range(x): z += i*i
>>> f(1000000)
>>> logtime('loop for a while #1',f,1000000)
>>> logtime2('loop for a while #2',f,1000000)
>>> logtimelim(1.0,'loop for a while #3',f,1000000)
>>> logtimelim2(2.0,'loop for a while #4',f,1000000)
>>> logtimelim2(2.0,'loop for a while #5',f,10000000)
>>> lograte2(123456,'loop for a while #6',f,123456)

"""

import time

# importing module must initialize logging

import logging
log = logging.getLogger('logtime')
#log.setLevel(1)

__all__ = ('timeit','logtime','logtime2',
            'logtimelim','logtimelim2',
            'lograte','lograte2',
            'set_timelogger','get_timelogger')

########################################################################
# integrated logging and timing

def get_timelogger(): return set_timelogger()
def set_timelogger(logger=None):
  '''assign logger to use for logtime functions

  (TODO: this is not elegant, need proper way to do this'''
  global log
  oldlogger = log
  if logger is not None:
    log = logger
  return oldlogger

def timeit(func,*args,**kwargs):
  '''return elapsed time and output of function call'''
  _t0 = time.time()
  out = func(*args,**kwargs)
  _t1 = time.time()
  secs = _t1 - _t0
  return (secs,out)

def logtime(desc,func,*args,**kwargs):
  '''log time at end of function call, returning function result'''
  return logtimelim(0.0,desc,func,*args,**kwargs)

def logtimelim(lim,desc,func,*args,**kwargs):
  '''log time of function call, only when threshold exceeded'''
  (secs,out) = timeit(func,*args,**kwargs)
  if secs >= lim:
    if desc is None: desc = str(func)
    log.info('time=%1.2f to %s',secs,desc)
  return out


def logtime2(desc,func,*args,**kwargs):
  '''same as logtime(), but also logs start of function call'''
  return logtimelim2(0.0,desc,func,*args,**kwargs)

def logtimelim2(lim,desc,func,*args,**kwargs):
  '''log start of function call, but end time only when threshold exceeded'''
  if desc is None: desc = str(func)
  log.info('starting to %s',desc)
  return logtimelim(lim,desc,func,*args,**kwargs)


def lograte(count,desc,func,*args,**kwargs):
  '''log time of function call, including rate calculation'''
  (secs,out) = timeit(func,*args,**kwargs)
  if desc is None: desc = str(func)
  try:
    rate = '%1.2f/s' % (float(count)/secs)
  except ZeroDivisionError:
    rate = 'NAN'
  log.info('time=%1.2f to %s (rate=%s for count=%s)',secs,desc,rate,count)
  return out

def lograte2(count,desc,func,*args,**kwargs):
  '''same as lograte(), but also logs start of function call'''
  if desc is None: desc = str(func)
  log.info('starting to %s (count=%s)',desc,count)
  return lograte(count,desc,func,*args,**kwargs)


#################################################################

if __name__ == "__main__":
  import doctest
  doctest.testmod()

#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
