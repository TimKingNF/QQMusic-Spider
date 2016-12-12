#! python
#coding=utf-8
from qq_spider import *
from httplib import HTTPConnection, HTTPSConnection
from time import time, strftime, localtime
from traceback import print_exc, format_exception
from collections import OrderedDict
import re, multiprocessing, os, sys, json, StringIO, gzip
try:
  import xml.etree.cElementTree as ET
except ImportError:
  import xml.etree.ElementTree as ET
reload(sys)
sys.setdefaultencoding('utf8')

# 爬取歌手 个人资料
# https://c.y.qq.com/splcloud/fcgi-bin/fcg_get_singer_desc.fcg?singermid=0020PeOh4ZaCw1&utf8=1&outCharset=utf-8&format=xml&r=1481196890095

# 歌手关注
# https://c.y.qq.com/rsc/fcgi-bin/fcg_order_singer_getnum.fcg?g_tk=1559226564&jsonpCallback=orderNum0020PeOh4ZaCw11481196890079&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0&singermid=0020PeOh4ZaCw1&utf8=1&rnd=1481196890079

# 歌手单曲
# https://c.y.qq.com/v8/fcg-bin/fcg_v8_singer_track_cp.fcg?g_tk=1559226564&jsonpCallback=MusicJsonCallbacksinger_track&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0&singermid=0020PeOh4ZaCw1&order=listen&begin=0&num=30&songstatus=1

# 歌手专辑
# https://c.y.qq.com/v8/fcg-bin/fcg_v8_singer_album.fcg?format=jsonp&platform=yqq&singermid=0020PeOh4ZaCw1&order=time&begin=0&num=5&g_tk=1559226564&jsonpCallback=singeralbumlistJsonCallback&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0

# 歌手MV
# https://c.y.qq.com/mv/fcgi-bin/fcg_singer_mv.fcg?cid=205360581&singermid=0020PeOh4ZaCw1&order=listen&begin=0&num=5&g_tk=1559226564&jsonpCallback=singermvlistJsonCallback&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0

# 档次爬取时间
SPIDER_TIME = strftime('%Y-%m-%d', localtime(time()))

# 格式化输出文件 间隔符号
SPILIT_ICON = "$^"

# 爬取歌手列表
# params[0] => A, B, C, D ...
# params[1] => 1, 2, 3, 4 ...
SPIDER_SINGERLIST_CONFIG = {
    "host": "c.y.qq.com",
    "url": "/v8/fcg-bin/v8.fcg?channel=singer&page=list&key=all_all_%s&pagesize=100&pagenum=%d&g_tk=1923362028&jsonpCallback=GetSingerListCallback&loginUin=11838706&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0"
}

# 爬取歌手 配置
# params[0] => singer_mid
# params[1] => unix_time
SPIDER_SINGERPROFILE_CONFIG = {
    "host": "c.y.qq.com",
    "url": "/splcloud/fcgi-bin/fcg_get_singer_desc.fcg?singermid=%s&utf8=1&outCharset=utf-8&format=xml&r=%d",
}

# 爬取歌手 单曲
# params[0] => singer_mid
# params[1] => begin 序号
# params[2] => num 一页歌曲数量
SPIDER_SINGERTRACK_CONFIG = {
    "host": "c.y.qq.com",
    "url": '/v8/fcg-bin/fcg_v8_singer_track_cp.fcg?g_tk=1923362028&jsonpCallback=MusicJsonCallbacksinger_track&loginUin=11838706&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0&singermid=%s&order=listen&begin=%d&num=%d&songstatus=1'
}

# 爬取歌曲 资料
SPIDER_AUDIOPROFILE_CONFIG = {
    "host": "y.qq.com",
    "url": "/portal/song/%s.html"
}

# 爬取歌曲的 专辑资料
SPIDER_ALBUMPROFILE_CONFIG = {
    "host": "c.y.qq.com",
    "url": "/v8/fcg-bin/fcg_v8_album_info_cp.fcg?albummid=%s&g_tk=742303916&jsonpCallback=getAlbumInfoCallback&loginUin=2993104336&hostUin=0&format=jsonp&inCharset=utf8&outCharset=utf-8&notice=0&platform=yqq&needNewCode=0"
}

# 创建目录
dir_path = "./%s/" % SPIDER_TIME
if not os.path.exists(dir_path):
    os.mkdir(dir_path)

SINGER_PATH = r"./%s/singer.txt" % SPIDER_TIME
TRACK_PATH = r"./%s/track.txt" % SPIDER_TIME
# 创建文件
open(SINGER_PATH, "w")
open(TRACK_PATH, "w")
################################################################

def gzdecode(data) :
    compressedstream = StringIO.StringIO(data)
    gziper = gzip.GzipFile(fileobj=compressedstream)
    data2 = gziper.read()   # 读取解压缩后数据
    return data2

#   爬取 歌手 个人详细资历
#   singer (list) =>
#   歌手ID
#   歌手名称
#   歌手别名 (中文名)
#   mid
#   国籍
#   ...
def spider_singerProfile_task(host, url_config, headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    "Referer":  "https://c.y.qq.com/xhr_proxy_utf8.html",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "no-cache",
}, singer = OrderedDict(), url_params = {}):
    try:
        # 从url_params 中获取参数 解析填入 url_config
        url = url_config % (
            singer["mid"],
            url_params["time"]
        )
        full_url = "https://%s%s" % (host, url)
        retry_times = 50
        while retry_times: # 如果出现错误 可以重新执行
            conn = HTTPSConnection(host, timeout = 10)
            yield conn.request("GET", url, headers = headers)
            try:
                response = conn.getresponse()
            except:
                # 如果发生了网络IO错误 则 重试
                retry_times -= 1
                continue

            if not response.status is 200:
                retry_times -= 1
                continue
            data = response.read()
            conn.close()
            # 解析data, 数据格式为 XML
            root = ET.fromstring(data) #从字符串传递xml
            if not root.find("code").text is '0':
                retry_times -= 1
                continue
            singer_country = '' # 歌手国籍
            try:
                for v in root.find("data").find("info").find("basic").findall("item"):
                    if v.find("key").text == u"国籍":
                        singer_country = v.find("value").text
                        break
            except:
                pass
            # merge singer data
            singer["country"] = singer_country
            # 写文件
            def write():
                with open(SINGER_PATH, "ab+") as fp: # singer file fp
                    fp.seek(os.SEEK_END)
                    print >> fp,  SPILIT_ICON.join([str(singer[k]) for k in singer])
            yield write()
            yield TaskReturn(url = full_url, result = 1) # 爬取成功
        # 爬取失败
        yield TaskReturn(url = full_url, result = 0)
    except GeneratorExit:
        pass
    except:
        # print url
        # print_exc()
        exceptionType, value, tb = sys.exc_info()
        exception = format_exception(exceptionType, value, tb)
        yield TaskReturn(url = full_url, result = 0, exception = "".join(exception))


# 爬取歌手的所有单曲
def spider_singerTrack_task(host, url_config, headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "no-cache",
    'Accept-Encoding': 'gzip, deflate',
}, singer = OrderedDict(), url_params = {}):
    try:
        url = url_config % (
            singer["mid"],
            url_params["begin"],
            url_params["num"]
        )
        full_url = "http://%s%s" % (host, url)
        retry_times = 50
        while retry_times:
            conn = HTTPConnection(host, timeout=10)
            yield conn.request("GET", url, headers=headers)
            try:
                response = conn.getresponse()
            except:
                # 如果发生了网络IO错误 则 重试
                retry_times -= 1
                continue
            if not response.status is 200:
                retry_times -= 1
                continue
            data = gzdecode(response.read())
            conn.close()
            # 解析data, 数据格式为 json
            response_json = data[31:-1]
            track_json = json.loads(response_json)
            if not track_json.get("code") is 0:
                retry_times -= 1
                continue
            songs = track_json.get("data").get("list") # 解析出单曲列表

            for song in songs:
                # 获取歌曲ID
                songId = song.get("musicData").get("songid") if song.get("songid") is None else song.get("songid")
                if songId is None:
                    retry_times -= 1
                    continue
                # 获取歌曲名称
                songName = song.get("musicData").get("songname") if song.get("songname") is None else song.get("songname")
                if songName is None:
                    retry_times -= 1
                    continue
                # 获取专辑ID
                albumid = song.get("musicData").get("albumid") if song.get("musicData").get("albumid") else ""
                # 获取专辑名称
                albumname = song.get("musicData").get("albumname") if song.get("musicData").get("albumname") else ""
                # 获取歌曲时长, 毫秒
                duration = int(song.get("musicData").get("interval")) * 1000 if song.get("musicData").get("interval") else 0
                # 音频文件大小
                fileSize = song.get("musicData").get("size128") if song.get("size128") is None else song.get("size128")
                # 收听次数
                listenCount = song.get("Flisten_count1") if song.get("Flisten_count1") else 0
                # mp3Url, 歌曲的mid
                songmid = song.get("musicData").get("songmid") if song.get("songmid") is None else song.get("songmid")
                # playUrl, 歌曲的mid
                strMediaMid = song.get("musicData").get("strMediaMid") if song.get("strMediaMid") is None else song.get("strMediaMid")
                # 服务器爬取时间
                updateTime = SPIDER_TIME
                # 歌曲MID id 用于获取歌曲 发行时间 流派 语言等信息用
                songMid = song.get("musicData").get("songmid") if song.get("songmid") is None else song.get("songmid")
                audioMid = song.get("musicData").get("albummid") if song.get("albummid") is None else song.get("albummid")

                audioData = OrderedDict()
                audioData["audioId"] = songId # 歌曲id
                audioData["audioName"] = songName # 歌曲名称
                audioData["audioDes"] = '' # 歌曲介绍
                audioData["audioPic"] = '' # 歌曲图片
                audioData["albumIds"] = albumid # 专辑ID
                audioData["albumNmae"] = albumname # 专辑名称
                audioData["commentNum"] = 0 # 评论次数
                audioData["createTime"] = '' # 发行时间
                audioData["duration"] = duration # 歌曲时长
                audioData["fileSize"] = fileSize # 文件大小
                audioData["host"] = singer["id"] # 歌手id
                audioData["likedNum"] = 0 # 点赞次数
                audioData["listenNum"] = listenCount # 收听次数
                audioData["updateTime"] = updateTime # 服务器爬取时间
                audioData["orderNumber"] = 0 # 歌曲排序
                audioData["mp3_url"] = songmid
                audioData["play_url"] = strMediaMid
                audioData["language"] = '' # 歌曲语言
                audioData["genre"] = '' # 歌曲流派
                # 爬取歌曲资料
                yield Task(spider_audioProfile_task(
                    SPIDER_AUDIOPROFILE_CONFIG["host"],
                    SPIDER_AUDIOPROFILE_CONFIG["url"],
                    audio = audioData,
                    url_params = {
                        "songmid": songmid,
                        "albummid": audioMid,
                    }
                ))

            total = track_json.get("data").get("total") # 歌曲的总数
            # 判断是否需要爬取下一页
            if total > url_params["begin"]:
                yield Task(spider_singerTrack_task(
                    host,
                    url_config,
                    singer = singer,
                    url_params = {
                        "begin": (url_params["begin"] + 1) * url_params["num"],
                        "num": url_params["num"],
                    }
                ))
            yield TaskReturn(url = full_url, result = 1)
         # 爬取失败
        yield TaskReturn(url = full_url, result = 0)
    except GeneratorExit:
        pass
    except :
        # print url
        # print_exc()
        exceptionType, value, tb = sys.exc_info()
        exception = format_exception(exceptionType, value, tb)
        yield TaskReturn(url = full_url, result = 0, exception = "".join(exception))

# 爬取歌曲专辑消息
def spider_albumProfile_task(host, url_config, headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "no-cache",
    'Accept-Encoding': 'gzip, deflate',
}, url_params = {}, audio = OrderedDict()):
    try:
        url = url_config % url_params["albummid"]
        full_url = "http://%s%s" % (host, url)
        retry_times = 50

        while retry_times:
            conn = HTTPConnection(host, timeout=10)
            yield conn.request("GET", url, headers=headers)
            try:
                response = conn.getresponse()
            except:
                # 如果发生了网络IO错误 则 重试
                retry_times -= 1
                continue
            if response.status is not 200:
                retry_times -= 1
                continue
            data = gzdecode(response.read())
            conn.close()
            # 从 data 中取出数据, 数据格式为 json
            response_json = data[22:-1]
            album_info = json.loads(response_json)
            if not album_info.get("code") is 0:
                retry_times -= 1
                continue
            genre = album_info.get("data").get("genre") # 获取流派
            createTime = album_info.get("data").get("aDate") # 获取发行时间
            # merge data
            audio["genre"] = genre
            audio["createTime"] = createTime
            # write data
            def write():
                with open(TRACK_PATH, "ab+") as fp: # track file fp
                    fp.seek(os.SEEK_END)
                    print >> fp, SPILIT_ICON.join([str(audio[k]) for k in audio])
            yield write()
            yield TaskReturn(url = full_url, result = 1)
        yield TaskReturn(url = full_url, result = 0)
    except GeneratorExit:
        pass
    except:
        # print url
        # print_exc()
        exceptionType, value, tb = sys.exc_info()
        exception = format_exception(exceptionType, value, tb)
        yield TaskReturn(url = full_url, result = 0, exception = "".join(exception))

# 爬取歌手的单曲的语言信息
def spider_audioProfile_task(host, url_config, headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "no-cache",
    'Accept-Encoding': 'gzip, deflate',
}, url_params = {}, audio = OrderedDict()):
    try:
        url = url_config % url_params["songmid"]
        full_url = "http://%s%s" % (host, url)
        retry_times = 50
        while retry_times:
            conn = HTTPConnection(host, timeout=10)
            yield conn.request("GET", url, headers=headers)
            try:
                response = conn.getresponse()
            except:
                # 如果发生了网络IO错误 则 重试
                retry_times -= 1
                continue
            if response.status is not 200:
                retry_times -= 1
                continue
            data = gzdecode(response.read())
            conn.close()
            # 从 data 中取出数据, 数据格式为HTML
            lan = ''
            reg_data = re.findall(r"<li class=\".* js_lan\">(.+)<\/li>", data)
            if reg_data:
                lan = reg_data.pop(0).decode("utf8")[3:] if reg_data[0] else ''
            # merge data
            audio["language"] = lan
            # 获取专辑信息
            yield Task(spider_albumProfile_task(
                SPIDER_ALBUMPROFILE_CONFIG["host"],
                SPIDER_ALBUMPROFILE_CONFIG["url"],
                audio = audio,
                url_params = {
                    "albummid": url_params["albummid"]
                }
            ))
            yield TaskReturn(url = full_url, result = 1)
        yield TaskReturn(url = full_url, result = 0)
    except GeneratorExit:
        pass
    except:
        # print url
        # print_exc()
        exceptionType, value, tb = sys.exc_info()
        exception = format_exception(exceptionType, value, tb)
        yield TaskReturn(url = full_url, result = 0, exception = "".join(exception))


# QQ 爬取 A-Z 下所有的 歌手
def spider_singerList_task(host, url_config, headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0",
    "Accept-Language": "zh-CN,zh;q=0.8",
    "Cache-Control": "no-cache",
    'Accept-Encoding': 'gzip, deflate',
}, url_params = {}):
    try:
        url = url_config % (
            url_params["index"],
            url_params["pagenum"]
        ) # 填入参数
        full_url = "http://%s%s" % (host, url)
        retry_times = 50
        while retry_times:
            conn = HTTPConnection(host, timeout=10)
            yield conn.request("GET", url, headers=headers)
            try:
                response = conn.getresponse()
            except:
                # 如果发生了网络IO错误 则 重试
                retry_times -= 1
                continue
            if response.status is not 200:
                retry_times -= 1
                continue
            data = gzdecode(response.read())
            conn.close()
            # 解析 data, 数据格式为 XML
            response_json = data[23:-1] # 截取字符串
            if response_json is '':
                return # 当再也没有下一页时 结束
            singerList = json.loads(response_json)
            singers = singerList.get("data").get("list")
            for singer in singers:
                # ID, 歌手名称, 歌手别名, 歌手MID(用于换算),
                singer_data = OrderedDict()
                singer_data["id"] = singer.get("Fsinger_id")
                singer_data["name"] = singer.get("Fsinger_name")
                singer_data["other_name"] = singer.get("Fother_name")
                singer_data["mid"] = singer.get("Fsinger_mid")

                # 将元素作为另一个协程的数据返回, 同时填入请求的url参数
                # 爬取歌手资料
                yield Task(spider_singerProfile_task(
                    SPIDER_SINGERPROFILE_CONFIG["host"],
                    SPIDER_SINGERPROFILE_CONFIG["url"],
                    singer = singer_data,
                    url_params = {
                        "time": time() * 1000
                    }
                ))
                # 爬取歌手的单曲
                yield Task(spider_singerTrack_task(
                    SPIDER_SINGERTRACK_CONFIG["host"],
                    SPIDER_SINGERTRACK_CONFIG["url"],
                    singer = singer_data,
                    url_params = {
                        "begin": 0, # 从哪里开始
                        "num": 1000, # 一次拉多少歌曲
                    }
                ))
                # return
            #   爬取下一页
            yield Task(spider_singerList_task(
                SPIDER_SINGERLIST_CONFIG["host"],
                SPIDER_SINGERLIST_CONFIG["url"],
                url_params = {
                    "index": url_params["index"], # A, B, C, D...
                    "pagenum": url_params["pagenum"] + 1 # 页数
                }
            ))
            yield TaskReturn(url = full_url, result = 1)
        # 爬取失败
        yield TaskReturn(url = full_url, result = 0)
    except GeneratorExit:
        pass
    except:
        # print url
        # print_exc()
        exceptionType, value, tb = sys.exc_info()
        exception = format_exception(exceptionType, value, tb)
        yield TaskReturn(url = full_url, result = 0, exception = "".join(exception))


################################################################

def Process_Start(index):
    scheduler = Scheduler()
    task = Task(spider_singerList_task(
        SPIDER_SINGERLIST_CONFIG["host"],
        SPIDER_SINGERLIST_CONFIG["url"],
        url_params = {
            "index": index,
            "pagenum": 1,
        }
    ))
    scheduler.put(task)
    scheduler.schedule()

if __name__ == '__main__':
    startTime = time()
    # 从 A-Z,  9
    jobs = []
    indexs = [i for i in range(65, 91)] + [57]
    # indexs = [57]
    for i in indexs:
        p = multiprocessing.Process(target=Process_Start, args=(chr(i),))
        p.start()
        jobs.append(p)

    for p in jobs:
        p.join()
    endTime = time()
    print "\nCOST", (endTime - startTime) , "s"
