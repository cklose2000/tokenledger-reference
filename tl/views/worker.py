"""Fresh benchmark processes prevent earlier Arrow heaps contaminating trials."""
import json
import os
import subprocess
import sys
import tempfile
from time import perf_counter

from tl.stream import ValidationError


def peak_memory():
    """OS high-water mark for this process, excluding child dbt processes."""
    if os.name=='nt':
        import ctypes
        from ctypes import wintypes
        class Counters(ctypes.Structure):
            _fields_=[('cb',wintypes.DWORD),('faults',wintypes.DWORD),
                *[(name,ctypes.c_size_t) for name in ('peak_working_set','working_set','peak_paged_pool',
                'paged_pool','peak_nonpaged_pool','nonpaged_pool','pagefile','peak_pagefile','private')]]
        counters=Counters();counters.cb=ctypes.sizeof(counters)
        kernel=ctypes.WinDLL('kernel32',use_last_error=True)
        kernel.GetCurrentProcess.restype=wintypes.HANDLE
        psapi=ctypes.WinDLL('psapi',use_last_error=True)
        psapi.GetProcessMemoryInfo.argtypes=[wintypes.HANDLE,ctypes.c_void_p,wintypes.DWORD]
        if not psapi.GetProcessMemoryInfo(kernel.GetCurrentProcess(),ctypes.byref(counters),counters.cb):
            return {'status':'unavailable','error_code':ctypes.get_last_error()}
        return dict(status='measured',peak_rss_bytes=counters.peak_working_set,
                    peak_commit_bytes=counters.peak_pagefile,scope='worker process only; child dbt excluded')
    import resource
    value=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return dict(status='measured',peak_rss_bytes=value if sys.platform=='darwin' else value*1024,
                scope='worker process only; child dbt excluded')


def run_worker(operation,request,progress=None):
    """Execute trusted local code. A receipt never supplies executable code."""
    tick=perf_counter();result=None
    with tempfile.TemporaryFile(mode='w+t',encoding='utf-8') as errors:
        process=subprocess.Popen([sys.executable,'-m','tl.views.worker',operation],
            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=errors,text=True,encoding='utf-8',
            creationflags=getattr(subprocess,'CREATE_NO_WINDOW',0))
        process.stdin.write(json.dumps(request,default=str));process.stdin.close()
        try:
            for line in process.stdout:
                event=json.loads(line)
                if 'progress' in event:
                    if progress: progress(event['progress'])
                elif 'result' in event: result=event
            code=process.wait()
        except BaseException:
            process.kill();process.wait();raise
        finally: process.stdout.close()
        errors.seek(0);detail=errors.read()
    if code or result is None:
        raise ValidationError('native benchmark worker failed: '+detail[-4000:])
    observation=dict(wall_seconds=perf_counter()-tick,pid=process.pid,**result['process_observation'])
    return result['result'],observation


def main():
    from tl.views.benchmark import compare,replay,_prepare_source
    from tl.views.engine import profile,replay as replay_profile
    choices={'compare':compare,'profile':profile,'prepare_source':_prepare_source,
             'replay_compare':replay,'replay_profile':replay_profile}
    operation=sys.argv[1]
    if operation not in choices: raise ValidationError('unsupported native worker operation')
    request=json.load(sys.stdin)
    def progress(value): print(json.dumps({'progress':value}),flush=True)
    if operation in ('compare','profile'): request['progress']=progress
    result=choices[operation](**request)
    print(json.dumps(dict(result=result,process_observation=peak_memory()),default=str),flush=True)


if __name__=='__main__': main()
