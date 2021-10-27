"""
binace usdt swap
"""

import urllib.request
import json
from datetime import datetime, timedelta
from logger import *
import redis
import time

URL = 'https://www.binance.com/fapi/v1/premiumIndex'
COST_FEE_RATE = (0.04)*2*0.01

def get_funding_rate() ->float:
  try:
    req = urllib.request.Request(URL, method='GET')
    response = urllib.request.urlopen(req)
    jdata = json.loads(response.read().decode('utf-8'))

    osd = {}
    old = {}
    for item in jdata:
      symbol = item['symbol']
      funding_rate = float(item['lastFundingRate'])
      if -COST_FEE_RATE <= funding_rate <= COST_FEE_RATE:
        continue

      if funding_rate > COST_FEE_RATE:
        osd[symbol] = funding_rate
      if funding_rate < -COST_FEE_RATE:
        old[symbol] = funding_rate
    osd={k: v for k, v in sorted(osd.items(), key=lambda item: item[1])}
    old={k: v for k, v in sorted(old.items(), key=lambda item: item[1])}
    return osd,old
  except Exception as e:
    logger.error(e)
    return None,None

def get_sleep_seconds():
  now_time = datetime.now()
  settle_1 = now_time.replace(hour=0, minute=0, second=0)
  settle_2 = settle_1 + timedelta(hours=7)
  settle_3 = settle_1 + timedelta(hours=15)
  settle_4 = settle_1 + timedelta(hours=23)
  settle_5 = settle_1 + timedelta(hours=31)

  sleep_time = None
  if now_time > settle_4:
    sleep_time = settle_5 - now_time
  elif now_time > settle_3:
    sleep_time = settle_4 - now_time
  elif now_time > settle_2:
    sleep_time = settle_3 - now_time
  elif now_time > settle_1:
    sleep_time = settle_2 - now_time
  else:
    pass
  if sleep_time is None:
    return 0
  return sleep_time.total_seconds()

def send_email(content):
  lines_content = "3140618@163.com\n\nu-swap-isolated funding rate\n\n{0}".format(content)
  rc = redis.StrictRedis(host='127.0.0.1', port='6379')
  rc.publish("email", lines_content)

if __name__ == '__main__':
  while True:
    open_short,open_long = get_funding_rate()
    if open_short is not None or open_long is not None:
      msg = 'open short:' + json.dumps(open_short) + ';open long:' + json.dumps(open_long)
      send_email(msg)
      logger.info(msg)
    sleep_time = get_sleep_seconds()
    time.sleep(sleep_time)