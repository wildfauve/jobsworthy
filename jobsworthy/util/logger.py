from typing import Dict, Optional, Union, Any, Callable
from pino import pino
import json
import time

from . import json_util

# public interface
def info(msg: str, observer: Optional = None, status: str = 'ok', ctx: Dict[str, str] = {}) -> None:
    _log('info', msg, observer, status, ctx)


def _log(level: str, msg: str, observer: Any, status: str, ctx: Dict[str, str]) -> None:
    if level not in level_functions.keys():
        return
    level_functions.get(level, info)(logger(), msg, meta(observer, status, ctx))



def with_perf_log(perf_log_type: str = None, name: str = None, callback: Callable = None):
    """
    Decorator which wraps the fn in a timer and writes a performance log
    """
    def inner(fn):
        def invoke(*args, **kwargs):
            post_log_callback = kwargs.get('callback', None) or callback
            t1 = time.time()
            result = fn(*args, **kwargs)
            t2 = time.time()
            if perf_log_type == 'http' and 'name' in kwargs:
                fn_name = kwargs['name']
            else:
                fn_name = name or fn.__name__
            perf_log(fn=fn_name, delta_t=(t2-t1)*1000.0, callback=post_log_callback)
            return result
        return invoke
    return inner


def custom_pino_dump_fn(json_log):
    return json.dumps(json_log, cls=json_util.CustomLogEncoder)

def logger():
    return pino(bindings={"apptype": "prototype", "context": "main"}, dump_function=custom_pino_dump_fn)

def _info(lgr, msg: str, meta: Dict) -> None:
    lgr.info(meta, msg)

def perf_log(fn: str, delta_t: float, callback: Callable):
    if callback:
        callback(fn, delta_t)
    info("PerfLog", ctx={'fn': fn, 'delta_t': delta_t})

def meta(observer, status: Union[str, int], ctx: Dict):
    return {**trace_meta(observer), **{'ctx': ctx}, **{'status': status}}

def trace_meta(observer):
    return observer.serialise() if observer else {}


level_functions = {'info': _info}
