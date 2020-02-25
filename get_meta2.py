import logging
logging.basicConfig(filename = 'search_log.txt', level = logging.DEBUG, filemode='w')
import asyncio
import aiohttp
import motor.motor_asyncio
import json
from pyquery import PyQuery as pq
import re
from pymongo import MongoClient
from multiprocessing import Queue

logging.basicConfig(filename='search_res_log.txt', level = logging.DEBUG, filemode = 'w')
from concurrent.futures import ProcessPoolExecutor
import multiprocessing

async def get_page(href='',proxy=None,redo=0,request_type='GET'):
    async with aiohttp.ClientSession() as client:
        logging.info('Hitting API Url : {0}'.format(href))
        response = await  client.request('{}'.format(request_type), href, proxy=proxy)
        logging.info('Status for {} : {}'.format(href,response.status))
        if response.status!= 200 and redo < 10:
            redo = redo + 1
            logging.warning("Response Code:" + str(response.status) +"received")
            return await get_json_page(href=href,proxy=None, redo=redo)
        else:
            return await response.text()

async def transform(kw):
    return '+'.join(kw.split('-'))

async def issue_search(kw_queue):
    await asyncio.sleep(0.2)
    kw = await kw_queue.get()
    kw_actual = await transform(kw)
    #print(kw_actual)
    pagination_count = 0
    first_url = "https://www.salary.com/tools/salary-calculator/search?keyword="+kw_actual+"&location=&page=0&selectedjobcodes="
    #print(url)
    first_page_html = await get_page(first_url)
    pq_obj = pq(first_page_html)
    href_list = []
    level2_title_list = []
    n_pages = pq_obj("body").children("div.sa-layout-container.padding-top15").children("div.sa-layout").children(
        "div.sa-layout-2a-a").children("div.sa-layout-section.border-top-none.sal-border-bottom").children(
        "nav#cityjobResultPagination").children("ul.pagination").children()[-1]
    last_link = pq(n_pages).find("a").attr("href")
    ll_list = re.findall("&page=.*&", last_link)
    last_pageno = int(ll_list[0][6:-1])
    #print(last_pageno)
    for i in range(last_pageno):
        #print(pagination_count)
        new_url = "https://www.salary.com/tools/salary-calculator/search?keyword="+kw_actual+"&location=&page=" + str(i)+"&selectedjobcodes="
        new_page_html = await get_page(new_url)
        #print(new_page_html)
        pq_obj = pq(new_page_html)
        a_list = pq_obj("body").children("div.sa-layout-container.padding-top15").children("div.sa-layout").children(
            "div.sa-layout-2a-a").children("div.sa-layout-section.border-top-none.sal-border-bottom").children(
            "div.sal-popluar-skills.margin-top30").children("div.margin-bottom10.font-semibold.sal-jobtitle").find(
            "a.a-color.font-semibold.margin-right10")
        """descs = pq_obj("body").children("div.sa-layout-container.padding-top15").children("div.sa-layout").children(
            "div.sa-layout-2a-a").children("div.sa-layout-section.border-top-none.sal-border-bottom").children(
            "div.sal-popluar-skills.margin-top30")
        alt_job_titles = pq_obj("body").children("div.sa-layout-container.padding-top15").children("div.sa-layout").children(
            "div.sa-layout-2a-a").children("div.sa-layout-section.border-top-none.sal-border-bottom").children(
            "div.sal-popluar-skills.margin-top30")"""
        for i in a_list:
            expr = pq(i).attr('href')
            link_title_string = re.findall("'https://www.salary.com/tools/salary-calculator/.*\'", expr)
            #print(type(link_title_string[0]))
            link, title = link_title_string[0].split(',', 1)
            #print(link, title)
            href_list.append(link[1:-1])
            level2_title_list.append(title[1:-1])
    #print(href_list)
    #print(level2_title_list)
    jt_2 = []
    for i in range(len(href_list)):
        new_dict = {}
        new_dict['JobRole'] = level2_title_list[i]
        new_dict['Link'] = href_list[i]
        jt_2.append(new_dict)
    client = motor.motor_asyncio.AsyncIOMotorClient()
    harvests_db = client['salarydotcom']
    consul2_coll = harvests_db['consul2']
    for i in jt_2:
        consul2_dict = {}
        consul2_dict['meta'] = kw
        consul2_dict['data'] = i
        logging.info(f'Inserting {consul2_dict}')
        x = await consul2_coll.find_one_and_update(
            {'data.JobRole': consul2_dict['data']['JobRole']},
            {'$set': consul2_dict},
            upsert=True)
        print(x)


async def get_search_res_queue(kw_queue, process_queue_size):
    search_queue = asyncio.Queue()
    for i in range(process_queue_size):
        if(not kw_queue.empty()):
            await search_queue.put(kw_queue.get())
    logging.info(f'Initiated async queues of {process_queue_size}')
    print(search_queue.qsize())
    tasks = []
    div_factor = 30
    times = search_queue.qsize() // div_factor
    for _ in range(times + 1):
        await asyncio.sleep(0.2)
        for i in range(times):
            task = asyncio.Task(issue_search(search_queue))
            tasks.append(task)
        await asyncio.gather(*tasks)

def driver(process_queue_size, kw_queue):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(get_search_res_queue(kw_queue, process_queue_size))


def gen_search_titles(no_processes):
    kw_queue = get_keyword_q()
    logging.info(f'Got Queue of size:{(kw_queue.qsize())}')
    process_queue_size = (kw_queue.qsize() // no_processes) + 1
    with multiprocessing.Pool(no_processes) as p:
        logging.info(f'Initiating {no_processes} pool workers')
        multi = [p.apply_async(driver, (process_queue_size, kw_queue, )) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()

def get_keyword_q():
    client = MongoClient()
    harvests_db = client['salarydotcom']
    consul1_coll = harvests_db['consul1']
    keyword_queue = multiprocessing.Manager().Queue()
    res = consul1_coll.find({})
    for i in list(res):
        keyword_queue.put(i['SEOFriendlyJobTitle'])
    return keyword_queue



if __name__=='__main__':
    PROCESSES = 16
    gen_search_titles(PROCESSES)
