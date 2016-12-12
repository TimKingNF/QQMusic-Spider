#!/usr/bin/python
import commands,string, os

out = commands.getoutput("ps -ef|grep qq_main.py|grep -v grep|grep -v PPID|awk '{ print $2}'")
pids = out.split("\n")

for i in pids:
    os.system("kill -9 %s" % i)
