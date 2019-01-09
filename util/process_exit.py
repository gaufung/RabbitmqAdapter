# -*- coding:utf-8 -*-
import os
import signal
import psutil


def exit():
    try:
        parent = psutil.Process(os.getpid())
        children = parent.children(recursive=True)
        for process in children:
            process.send_signal(signal.SIGTERM)
        os._exit(0)
    except Exception:
        pass
