"""Get location variant data"""
import logging
import asyncio
import aiohttp
import motor.motor_asyncio
import json
from pyquery import PyQuery as pq
import re
from datetime import date, datetime
import time
from pymongo import MongoClient
import multiprocessing
logging.basicConfig(filename = 'search_log.txt', level = logging.DEBUG, filemode='w')

from get_salary import get_page, get_sal_dict

async def get_salary_from_page_w_locs(data_queue, sal_type):
    meta_data = await data_queue.get()
    #print(meta_data)
    yoe = {'&yrs=0':'<1','&yrs=1.5':'1-2','&yrs=3.5':'3-4','&yrs=5.5':'5-6','&yrs=8':'7-9','&yrs=12':'10-14','&yrs=17':'15-19','&yrs=20':'20+'}
    for y in list(yoe.keys()):
        hit_url = meta_data['Location Details']['loc_link']+"y"+"&view=table"
        #print(hit_url)
        meta1 = meta_data['meta2']['meta']
        meta2 = meta_data['meta2']
        page_html = await get_page(hit_url)
        pq_obj = pq(page_html)
        table = pq_obj("div#divtable").children("div.padding-left15.padding-right5").children("table").children("tbody")
        salary_dict = await get_sal_dict(table)
        client = motor.motor_asyncio.AsyncIOMotorClient()
        harvests_db = client['salarydotcom']
        data_coll = harvests_db['salary_data_loc'+sal_type]
        data_dict = {"meta3":meta_data, "data":{"JobRole": meta_data['Job Role'], "YOE":yoe[y], "Location": meta_data['Location Details']['location_name'],"salary_data":salary_dict}}
        logging.info(f'DP OP: Inserting data for {data_dict["data"]["JobRole"]}')
        await data_coll.find_one_and_update({'data.JobRole':meta_data['Job Role']},{'$set':data_dict}, upsert=True)
        #print(data_dict)



async def make_tasks_and_exc(process_queue_size, data_queue, sal_type):
    async_queue = asyncio.Queue()
    for i in range(process_queue_size):
        if(not data_queue.empty()):
            await async_queue.put(data_queue.get())
    logging.info(f'Initializing async worker queue of size: {(async_queue.qsize())}')
    tasks = []
    div_factor = 30
    times = async_queue.qsize() // div_factor
    for _ in range(times + 1):
        await asyncio.sleep(0.2)
        logging.info(f'Making {times} async tasks')
        for i in range(times):
            task = asyncio.Task(get_salary_from_page_w_locs(async_queue, sal_type))
            tasks.append(task)
        await asyncio.gather(*tasks)

def driver_sal_locs(process_queue_size, data_queue, sal_type):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(make_tasks_and_exc(process_queue_size, data_queue, sal_type))


def get_salary_locs(sal_type, no_processes):
    data_queue = get_data_q(sal_type)
    logging.info(f'Master Queue Size:{(data_queue.qsize())}')
    process_queue_size = (data_queue.qsize() // no_processes) + 1
    with multiprocessing.Pool(no_processes) as p:
        logging.info(f'Initiating pool of {no_processes} worker processes')
        multi = [p.apply_async(driver_sal_locs, (process_queue_size, data_queue, sal_type,)) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()

def get_data_q(sal_type):
    client = MongoClient()
    harvests_db = client['salarydotcom']
    consul2_coll = harvests_db['consul3'+sal_type]
    data_queue = multiprocessing.Manager().Queue()
    res = consul2_coll.find({})
    for i in list(res):
        data_queue.put(i)
    return data_queue

if __name__=='__main__':
    PROCESSES = 16
    sal_type = 'base'
    get_salary_locs(sal_type, PROCESSES)