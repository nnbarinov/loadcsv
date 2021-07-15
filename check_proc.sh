#!/bin/bash

if [ `ps -ef | grep -v grep | grep -E 'prm.batch.listener|icsmpgum|ICRATER|RELEASE' | wc -l` -eq 0 ]
then
  exit 0
else
  exit 1
fi