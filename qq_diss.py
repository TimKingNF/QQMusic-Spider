#! python
#coding=utf-8
from qq_spider import *
from traceback import print_exc, format_exception
from collections import OrderedDict
import json, os, sys, socket
reload(sys)
sys.setdefaultencoding('utf8')


# 爬取分类
# https://c.y.qq.com/splcloud/fcgi-bin/fcg_get_diss_tag_conf.fcg?g_tk=1638255111&jsonpCallback=getPlaylistTags&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0

# 爬取歌单
# https://c.y.qq.com/splcloud/fcgi-bin/fcg_get_diss_by_tag.fcg?rnd=0.8666027731002006&g_tk=1638255111&jsonpCallback=getPlaylist&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0&categoryId=10000000&sortId=5&sin=0&ein=29

# 爬取歌单歌曲列表
# https://c.y.qq.com/qzone/fcg-bin/fcg_ucc_getcdinfo_byids_cp.fcg?type=1&json=1&utf8=1&onlysong=0&disstid=2339571566&format=jsonp&g_tk=1449993694&jsonpCallback=playlistinfoCallback&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0

# 当次爬取时间
SPIDER_TIME = strftime('%Y-%m-%d', localtime(time()))

# 格式化输出文件 间隔符号
SPILIT_ICON = "$^"

# 爬取分类
SPIDER_CATEGORY_CONFIG = {
    "host": "c.y.qq.com",
    "url": "/splcloud/fcgi-bin/fcg_get_diss_tag_conf.fcg?g_tk=1638255111&jsonpCallback=getPlaylistTags&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0"
}

# 爬取歌单列表
SPIDER_DISSLIST_CONFIG = {
    "host": "c.y.qq.com",
    "url": "/splcloud/fcgi-bin/fcg_get_diss_by_tag.fcg?rnd=0.8666027731002006&g_tk=1638255111&jsonpCallback=getPlaylist&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0&categoryId=%s&sortId=5&sin=%d&ein=%d"
}

# 爬取歌单里边的歌曲列表
SPIDER_DISSINFO_CONFIG = {
    "host": "c.y.qq.com",
    "url": "/qzone/fcg-bin/fcg_ucc_getcdinfo_byids_cp.fcg?type=1&json=1&utf8=1&onlysong=0&disstid=%s&format=jsonp&g_tk=1449993694&jsonpCallback=playlistinfoCallback&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0"
}

# 创建目录
dir_path = "./%s/" % SPIDER_TIME
if not os.path.exists(dir_path):
    os.mkdir(dir_path)

CATEGORY_PATH = r"./%s/category.txt" % SPIDER_TIME
DISS_PATH = r"./%s/diss.txt" % SPIDER_TIME
DISSINFO_PATH = r"./%s/diss_info.txt" % SPIDER_TIME
# 创建文件
open(CATEGORY_PATH, "w")
open(DISS_PATH, "w")
open(DISSINFO_PATH, "w")

################################################################

#   怕个歌单歌曲列表
def spider_dissInfo_task(host, url_config, headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    "Referer":  "https://c.y.qq.com/xhr_proxy_utf8.html",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "no-cache",
}, url_params = {}, diss = OrderedDict()):
    try:
        url = url_config % url_params["disstid"]
        full_url = "https://%s%s" % (host, url)
        retry_times = 50
        while retry_times:
            retry_times -= 1

            # 需要改写
            g = gevent.spawn(fetcher, host, url, scheme = "https", headers = headers)
            while not g.ready() is True:
                gevent.sleep(0.001) # 1毫秒
                ret = yield g

            if isinstance(ret, gevent.Greenlet): ret = ret.value
            if ret is None: continue
            (data, cost) = ret
            #######

            response_json = data[21:-1]
            try:
                diss_info = json.loads(response_json)
            except UnicodeDecodeError: # 如果发生解码错误
                continue

            if not diss_info.get("code") is 0:
                continue

            cdlist = diss_info.get("cdlist")
            if len(cdlist) > 0:
                songids = cdlist[0].get("songids")
                dissInfo = OrderedDict()
                dissInfo["disstid"] = url_params["disstid"]
                dissInfo["songids"] = songids
                diss["desc"] = cdlist[0].get("desc")

                def write_diss_info():
                    with open(DISSINFO_PATH, "ab+") as fp:
                        print >> fp, SPILIT_ICON.join([str(dissInfo[k]) for k in dissInfo])
                yield write_diss_info()

            # 将歌单存档
            def write_diss():
                with open(DISS_PATH, "ab+") as fp:
                    print >> fp, SPILIT_ICON.join([str(diss[k]) for k in diss])
            yield write_diss()

            yield TaskReturn(url = full_url, result = 1, retry_times = 50 - retry_times, http_cost = cost)
        yield TaskReturn(url = full_url, result = 0, retry_times = 50 - retry_times, http_cost = 0)
    except GeneratorExit:
        pass
    except:
        exceptionType, value, tb = sys.exc_info()
        exception = format_exception(exceptionType, value, tb)
        yield TaskReturn(url = full_url, result = 0, exception = "".join(exception))

#   爬取歌单列表
def spider_dissList_task(host, url_config, headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    "Referer":  "https://c.y.qq.com/xhr_proxy_utf8.html",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "no-cache",
}, url_params = {}, category = OrderedDict()):
    try:
        if url_params["sin"] > url_params["total"]: return
        url = url_config % (
            category["categoryId"],
            url_params["sin"],
            url_params["ein"],
        )
        full_url = "https://%s%s" % (host, url)
        retry_times = 50
        while retry_times:
            retry_times -= 1

            # 需要改写
            g = gevent.spawn(fetcher, host, url, scheme = "https", headers = headers)
            while not g.ready() is True:
                gevent.sleep(0.001) # 1毫秒
                ret = yield g

            if isinstance(ret, gevent.Greenlet): ret = ret.value
            if ret is None: continue
            (data, cost) = ret

            response_json = data[12:-1]
            try:
                diss_info = json.loads(response_json)
            except UnicodeDecodeError:
                continue
            if not diss_info.get("code") is 0:
                continue

            diss_list = diss_info.get("data").get("list")
            total_diss = diss_info.get("data").get("sum")
            for diss_item in diss_list:
                diss = OrderedDict()

                diss["dissid"] = diss_item.get("dissid")
                diss["dissname"] = diss_item.get("dissname")
                diss["imgurl"] = diss_item.get("imgurl")
                diss["followedNum"] = 0
                diss["listenNum"] = diss_item.get("listennum")
                diss["desc"] = diss_item.get("introduction")
                diss["artists"] = "" # 歌单创建人。 没有意义
                diss["categorys"] = category["categoryId"]
                diss["updateTime"] = diss_item.get("createtime")
                diss["local_update_time"] = SPIDER_TIME

                # 构建爬取歌单列表
                yield Task(spider_dissInfo_task(
                    SPIDER_DISSINFO_CONFIG["host"],
                    SPIDER_DISSINFO_CONFIG["url"],
                    diss = diss,
                    url_params = {
                        "disstid": diss["dissid"],
                    }
                ))

            # 爬取下一页
            yield Task(spider_dissList_task(
                SPIDER_DISSLIST_CONFIG["host"],
                SPIDER_DISSLIST_CONFIG["url"],
                category = category,
                url_params = {
                    "sin": url_params["page"] * 50,
                    "ein": (url_params["page"] + 1) * 50 - 1,
                    "page": url_params["page"] + 1,
                    "total": total_diss,
                }
            ))

            yield TaskReturn(url = full_url, result = 1, retry_times = 50 - retry_times, http_cost = cost)
        yield TaskReturn(url = full_url, result = 0, retry_times = 50 - retry_times, http_cost = 0)
    except GeneratorExit:
        pass
    except:
        exceptionType, value, tb = sys.exc_info()
        exception = format_exception(exceptionType, value, tb)
        yield TaskReturn(url = full_url, result = 0, exception = "".join(exception))


#   爬取分类
def spider_category_task(host, url, headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    "Referer":  "https://c.y.qq.com/xhr_proxy_utf8.html",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "no-cache",
}):
    try:
        full_url = "https://%s%s" % (host, url)
        retry_times = 50
        while retry_times:
            retry_times -= 1

            # 需要改写
            g = gevent.spawn(fetcher, host, url, scheme = "https", headers = headers)
            while not g.ready() is True:
                gevent.sleep(0.001) # 1毫秒
                ret = yield g

            if isinstance(ret, gevent.Greenlet): ret = ret.value
            if ret is None: continue
            (data, cost) = ret

            response_json = data[16:-1]
            try:
                category_info = json.loads(response_json)
            except UnicodeDecodeError:
                continue

            if not category_info.get("code") is 0:
                continue
            categories = category_info.get("data").get("categories")
            for category in categories:
                if category.get("usable") is 0: continue
                categoryGroupName = category.get("categoryGroupName")
                for category_item in category.get("items"):
                    if category_item.get("usable") is 0: continue

                    category = OrderedDict()
                    category["categoryId"] = category_item.get("categoryId")
                    category["categoryName"] = category_item.get("categoryName")
                    category["categoryGroupName"] = categoryGroupName

                    def write():
                        with open(CATEGORY_PATH, "ab+") as fp:
                            print >> fp, SPILIT_ICON.join([str(category[k]) for k in category])
                    yield write()

                    yield Task(spider_dissList_task(
                        SPIDER_DISSLIST_CONFIG["host"],
                        SPIDER_DISSLIST_CONFIG["url"],
                        category = category,
                        url_params = {
                            "sin": 0,
                            "ein": 49,
                            "page": 1,
                            "total": 0,
                        }
                    ))

            yield TaskReturn(url = full_url, result = 1, retry_times = 50 - retry_times, http_cost = cost)
        yield TaskReturn(url = full_url, result = 0, retry_times = 50 - retry_times, http_cost = 0)
    except GeneratorExit:
        pass
    except:
        exceptionType, value, tb = sys.exc_info()
        exception = format_exception(exceptionType, value, tb)
        yield TaskReturn(url = full_url, result = 0, exception = "".join(exception))

################################################################
def Process_Start():
    scheduler = Scheduler("./%s/runtime.log" % SPIDER_TIME)
    task = Task(spider_category_task(
        SPIDER_CATEGORY_CONFIG["host"],
        SPIDER_CATEGORY_CONFIG["url"],
    ))
    scheduler.put(task)
    scheduler.schedule()

if __name__ == '__main__':
    startTime = clock()
    Process_Start()
    endTime = clock()
    print "\nCOST", (endTime - startTime) , "s"
