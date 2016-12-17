#! python
#coding=utf-8
from traceback import print_exc
from time import time, sleep, strftime, localtime, clock
from gevent import monkey; monkey.patch_all()
import os, sys, socket, gevent, StringIO, gzip
from httplib import HTTPConnection, HTTPSConnection
reload(sys)
sys.setdefaultencoding('utf8')


SPIDER_TIME = strftime('%Y-%m-%d', localtime(time()))
dir_path = "./%s/" % SPIDER_TIME
if not os.path.exists(dir_path):
    os.mkdir(dir_path)

RUNTIME_PATH = "./%s/runtime_%d.log" % (SPIDER_TIME, int(time()))

#   终端染色
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

#   Task 结束
class TaskReturn:
    def __init__(self, **kargs):
        self.url = ''
        self.result = 0
        self.exception = None
        for k in kargs:
            setattr(self, k, kargs[k])

#   可供重载 TASK, 重写 _end
class Task(object):
    def __init__(self, func):
        self.coroutine = func
        self.finish = False
        self.scheduled = 0 # 受到调度的次数
        self.startTime = clock() # 开始时间

    def isFinish(self):
        return self.finish

    # 当run 执行完毕, 输入日志
    # ret => TaskReturn
    def _end(self, ret = None):
        self.finish = True
        endTime = clock()
        if not ret is None and isinstance(ret, TaskReturn):
            fp = open(RUNTIME_PATH, "ab+")
            log_template = "%s HTTPCOST %s ms, COST %s ms, SCHEDULED %s, SPIDER %s"
            if not ret.exception is None:
                log_template += ", EXCEPTION %s"
                cmd_log = log_template % (
                    Logger.high("【SUCCESS】") if ret.result is 1 else Logger.fail("【ERROR】"),
                    Logger.high(str(0)),
                    Logger.normal(str((endTime - self.startTime) * 1000)),
                    Logger.high(str(self.scheduled)),
                    Logger.normal(ret.url),
                    Logger.fail(ret.exception),
                )
                log = log_template % (
                    "【SUCCESS】" if ret.result is 1 else "【ERROR】",
                    str(0),
                    str((endTime - self.startTime) * 1000),
                    str(self.scheduled),
                    ret.url,
                    ret.exception,
                )
            else:
                log_template += ", RETRY %s"
                cmd_log = log_template % (
                    Logger.high("【SUCCESS】") if ret.result is 1 else Logger.fail("【ERROR】"),
                    Logger.normal(str(ret.http_cost)),
                    Logger.normal(str((endTime - self.startTime) * 1000)),
                    Logger.high(str(self.scheduled)),
                    Logger.normal(ret.url),
                    Logger.high(str(ret.retry_times))
                )
                log = log_template % (
                    "【SUCCESS】" if ret.result is 1 else "【ERROR】",
                    str(ret.http_cost),
                    str((endTime - self.startTime) * 1000),
                    str(self.scheduled),
                    ret.url,
                    str(ret.retry_times)
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

#    调度器
class Scheduler:
    def __init__(self, runtime_path = RUNTIME_PATH):
        self.queue = list()
        # 判断是否存在日志文件
        global RUNTIME_PATH
        RUNTIME_PATH = runtime_path
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
                # 如果返回的是异步任务
                if isinstance(ret, gevent.Greenlet):
                    try:
                        ret = ret.get(block = False)
                    except gevent.timeout.Timeout:
                        pass

                # 如果返回的是一个新的任务
                if isinstance(ret, Task): self.put(ret)
                # 如果有专门处理结果集的 任务
                if isinstance(ret, TaskReturn):
                    pass
                if not task.isFinish(): self.put(task, ret)
                else:
                    print "%s %s" % (Logger.normal("SCHEDULER"), Logger.high("【"+str(len(self.queue))+"】"))
                    task.close()
            else:
                print "END..."
                break

################################################################

#    爬取器
def fetcher(host, url, headers = {}, timeout = 10, method = "GET", scheme = "http"):
    try:
        startTime = clock()
        if scheme is "http": conn = HTTPConnection(host, timeout = timeout)
        elif scheme is "https": conn = HTTPSConnection(host, timeout = timeout)
        else: return None
        conn.request(method, url, headers = headers)
        response = conn.getresponse()
        endTime = clock()
        if not response.status is 200:
            return None
        data = response.read()
        conn.close()
        return (data, (endTime - startTime) * 1000)
    except:
        return None


#   gzip 解压
def gzdecode(data) :
    compressedstream = StringIO.StringIO(data)
    gziper = gzip.GzipFile(fileobj=compressedstream)
    data2 = gziper.read()   # 读取解压缩后数据
    return data2

################################################################

# 协程测试程序
def add():
    try:
        for i in range(5):
            print i-1
            n = yield i*i
            print('[CONSUMER] Consuming %s...' % n)
            # sleep(1)
        yield
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
    # 测试异步爬取
    # gevent.joinall([
    #     gevent.spawn(fetcher, 'c.y.qq.com', "/qzone/fcg-bin/fcg_ucc_getcdinfo_byids_cp.fcg?type=1&json=1&utf8=1&onlysong=0&disstid=1140866868&format=jsonp&g_tk=1449993694&jsonpCallback=playlistinfoCallback&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0", {
    #         "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    #         "Referer":  "https://c.y.qq.com/xhr_proxy_utf8.html",
    #         "Accept-Language": "zh-CN,zh;q=0.8",
    #         "Cache-Control": "no-cache",
    #     }) for i in range (1000)
    # ])
