#! python
#coding=utf-8
from traceback import print_exc
from time import time, sleep, strftime, localtime
import os, sys
reload(sys)
sys.setdefaultencoding('utf8')


SPIDER_TIME = strftime('%Y-%m-%d', localtime(time()))
RUNTIME_PATH = "./%s/runtime.log" % (SPIDER_TIME)

class Logger:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    @staticmethod
    def normal(info):
        return Logger.OKBLUE + info + Logger.ENDC
    @staticmethod
    def high(info):
        return Logger.OKGREEN + info + Logger.ENDC
    @staticmethod
    def fail(info):
        return Logger.FAIL + info + Logger.ENDC

class TaskReturn:
    def __init__(self, **kargs):
        self.url = ''
        self.result = 0
        self.exception = None
        for k in kargs:
            setattr(self, k, kargs[k])


# 可供重载 TASK, 重写 _end
class Task(object):
    def __init__(self, func):
        self.coroutine = func
        self.finish = False
        self.scheduled = 0 # 受到调度的次数
        self.startTime = time() # 开始时间

    def isFinish(self):
        return self.finish

    # 当run 执行完毕, 输入日志
    # ret => TaskReturn
    def _end(self, ret = None):
        self.finish = True
        if not ret is None and isinstance(ret, TaskReturn):
            fp = open(RUNTIME_PATH, "a")
            log_template = "%sCOST %s ms, SCHEDULED %s, SPIDER %s"
            if not ret.exception is None:
                log_template += ", EXCEPTION %s"
                cmd_log = log_template % (
                    Logger.high("【SUCCESS】") if ret.result is 1 else Logger.fail("【ERROR】"),
                    Logger.normal(str((time() - self.startTime) * 1000)),
                    Logger.high(str(self.scheduled)),
                    Logger.normal(ret.url),
                    Logger.fail(ret.exception),
                )
                log = log_template % (
                    "【SUCCESS】" if ret.result is 1 else "【ERROR】",
                    str((time() - self.startTime) * 1000),
                    str(self.scheduled),
                    ret.url,
                    ret.exception,
                )
            else:
                cmd_log = log_template % (
                    Logger.high("【SUCCESS】") if ret.result is 1 else Logger.fail("【ERROR】"),
                    Logger.normal(str((time() - self.startTime) * 1000)),
                    Logger.high(str(self.scheduled)),
                    Logger.normal(ret.url),
                )
                log = log_template % (
                    "【SUCCESS】" if ret.result is 1 else "【ERROR】",
                    str((time() - self.startTime) * 1000),
                    str(self.scheduled),
                    ret.url,
                )
            print cmd_log
            print >> fp, log

    def close(self):
        self.coroutine.close()

    def run(self, arg = None):
        self.scheduled += 1
        try:
            if arg is None:
                ret = self.coroutine.next()
            else:
                ret = self.coroutine.send(arg)
            if isinstance(ret, TaskReturn):
                self._end(ret)

            return ret
        except StopIteration:
            self._end()


class Scheduler:
    def __init__(self):
        self.queue = list()
        # 判断是否存在日志文件
        open(RUNTIME_PATH, "w")

    def put(self, task, ret = None):
        # self.queue.put( (task, ret) )
        self.queue.append( (task, ret) )

    def schedule(self):
        print "Start..."
        while 1:
            if self.queue:
                package = self.queue.pop(0)
                # package => (task, return)
                if not isinstance(package[0], Task): continue
                (task, arg) = package
                # 调度执行
                ret = task.run(arg)
                # 如果返回的是一个新的任务
                if isinstance(ret, Task): self.put(ret)
                # 如果有专门处理结果集的 任务
                if isinstance(ret, TaskReturn):
                    pass
                if not task.isFinish(): self.put(task, ret)
                else: task.close()
            else:
                print "END..."
                break

# 协程测试程序
def add():
    try:
        for i in range(5):
            print i-1
            n = yield i*i
            print('[CONSUMER] Consuming %s...' % n)
            # sleep(1)
        yield TaskReturn(url = "index.html", result = 1)
    except GeneratorExit:
        pass
    except:
        print_exc()


if __name__ == '__main__':
    # 测试程序
    scheduler = Scheduler()
    add_task = Task(add())
    scheduler.put(add_task)
    scheduler.schedule()
