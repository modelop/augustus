"""Run group of commands with optional setup and cleanup of named pipes

Patterns matching '%n' in the command strings are replaced with the
corresponding pipe name ('%0' -> first pipe, etc).

The last command in the command list is treated specially.
It determines the overall return status.  If it finishes
while there are other jobs still running then the other jobs
are killed (they may still be trying to feed data into the FIFOs).
If any command exits with a non-zero status,
then the remaining jobs are terminated.

"""

__copyright__ = """
Copyright (C) 2005-2006  Open Data ("Open Data" refers to
one or more of the following companies: Open Data Partners LLC,
Open Data Research LLC, or Open Data Capital LLC.)

This file is part of Augustus.

Augustus is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA
"""


import sys, os, re, time
from subprocess import Popen
import logging
logging.basicConfig()
log = logging.getLogger('runfifo')
log.setLevel(1)

#################################################################

def main():
  """handle user command when run as top level program"""
  from optparse import OptionParser, make_option

  usage = 'usage: %prog [options] commands(s)'
  version = "%prog 0.0 alpha"
  option_list = [
    make_option('-v','--verbose',action='count',default=1,help="make progress output more verbose"),
    make_option('-q','--quiet',action='store_false',dest='verbose',help="no progress messages"),

    make_option('-p','--pipes',metavar='NAMES',help='list of named pipes to create'),
    make_option('-f','--force',action='store_true',help='remove any pre-existing pipes'),
    make_option('-s','--shell',action='store_true',help='start jobs using shell'),

  ]

  parser = OptionParser(usage=usage,version=version,option_list=option_list)
  (opt,args) = parser.parse_args()

  log.setLevel(max(1,40 - (opt.verbose*10)))

  if not args:
    parser.print_help()
    sys.exit(0)

  if opt.pipes:
    argsep = re.compile(r'[,\s]+')
    opt.pipes = argsep.split(opt.pipes.strip())

  jobs = RunFifo(args,pipes=opt.pipes,force=opt.force,shell=opt.shell)
  try:
    status = jobs.run()
  except KeyboardInterrupt:
    log.warning('KeyboardInterrupt: cleaning up')
    jobs.cleanup()
    sys.exit(-2)

  sys.exit(status != 0)


#################################################################
# file reader subsystem

class RunFifo(object):
  """manage child process and associated named pipes

  """

  def __init__(self,cmds,pipes=None,force=False,shell=False):
    self.pipes = pipes or []
    self.force = force
    self.shell = shell
    for i,pipe in reversed(list(enumerate(self.pipes))):
      pat = re.compile('%%%s' % i)
      cmds = [pat.sub(pipe,cmd) for cmd in cmds]
    self.cmds = cmds
    self.pidmap = {}
    self.procs = []
    self.killing = False
    self.status = None

  def run(self,wait=True):
    self.pidmap = pidmap = {}
    self.procs = procs = []
    self.setup()
    for cmd in self.cmds:
      log.info('starting: %r',cmd)
      if self.shell:
        proc = Popen(cmd,shell=True)
      else:
        proc = Popen(cmd.split())
      procs.append(proc)
      pid = proc.pid
      pidmap[pid] = cmd
      log.debug('started (pid=%s): %r',pid,cmd)
    if wait:
      return self.wait()
    return self

  def wait(self):
    pidmap = self.pidmap
    while self._reap():
      log.info('waiting for %s child processes',len(pidmap))
      try:
        (pid,status) = os.wait()
      except OSError:
        #log.exception('running os.wait()')
        break
      self._reap(pid,status)
    if not self.killing:
      self._reap()
      log.info('finished, cleaning up')
      self.cleanup()
    lastproc = self.procs[-1]
    self.status = status = lastproc.returncode
    return self.status

  def _reap(self,pid=None,status=None):
    if pid is not None:
      for proc in self.procs:
        if proc.pid == pid:
          proc.returncode = status
          break
    pidmap = self.pidmap
    log.debug('____________reap____________')
    for n,proc in enumerate(self.procs):
      pid = proc.pid
      status = proc.poll()
      if status is None:
        # still running
        log.debug('still running (pid=%s): %r',pid,pidmap.get(pid))
        continue
      if pid not in pidmap:
        # already reported
        log.debug('previous exit(%s) (pid=%s): %r',proc.returncode,pid,pidmap.get(pid))
        continue
      cmd = pidmap.pop(pid)
      if proc.returncode == 0 or self.killing:
        log.info('exit(%s) (pid=%s): %r',proc.returncode,pid,cmd)
      else:
        log.warning('killing remaining jobs due to exit(%s) (pid=%s): %r',proc.returncode,pid,cmd)
        self.cleanup()
    lastproc = self.procs[-1]
    status = lastproc.returncode
    if status is not None and not self.killing and pidmap:
      log.info('killing remaining jobs due to completion of terminal job')
      self.cleanup()
    else:
      log.debug('jobs remaining: %r',pidmap.keys())
    return pidmap

  def setup(self):
    for pipe in self.pipes:
      if self.force and os.path.exists(pipe):
        log.info('removing existing: %s',pipe)
        os.unlink(pipe)
    for i,pipe in enumerate(self.pipes):
      log.info('creating pipe %s: %s',i,pipe)
      os.mkfifo(pipe)


  def __del__(self): return self.cleanup()
  def cleanup(self):
    if self.killing:
      return
    self.killing = True
    if self._reap():
      self.kill_procs(1)
      self._reap()
    for i,pipe in enumerate(self.pipes):
      if os.path.exists(pipe):
        log.info('removing pipe %s: %s',i,pipe)
        os.unlink(pipe)
    if self._reap():
      time.sleep(2)
      if self._reap():
        self.kill_procs(9)
        time.sleep(2)
      if self._reap():
        log.error('some procs could not be killed: %r',self.pidmap.keys())

  def kill_procs(self,signal=15):
    for n,proc in enumerate(self.procs):
      if proc.poll() is None:
        pid = proc.pid
        log.info('killing with signal=%s (pid=%s): %r',signal,pid,self.pidmap.get(pid))
        try:
          os.kill(pid,signal)
        except OSError:
          log.exception('killing with signal=%s (pid=%s): %r',signal,pid,self.pidmap.get(pid))
          if proc.returncode is None:
            proc.returncode = -signal
    



#################################################################

if __name__ == "__main__":
  main()

#################################################################
#################################################################
# vim:sw=2:sts=2:expandtab:shiftround
