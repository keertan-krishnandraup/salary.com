import logging
logging.basicConfig(filename = 'search_log.txt', level = logging.DEBUG, filemode='w')
import asyncio
import aiohttp
import motor.motor_asyncio
import json
from pyquery import PyQuery as pq
import re
from datetime import date, datetime
import multiprocessing
from pymongo import MongoClient

logging.basicConfig(filename='get_sal_log.txt', level = logging.DEBUG, filemode = 'w')

async def get_page(href='',proxy=None,redo=0,request_type='GET'):
    async with aiohttp.ClientSession() as client:
        logging.info('Hitting API Url : {0}'.format(href))
        response = await  client.request('{}'.format(request_type), href, proxy=proxy)
        logging.info('Status for {} : {}'.format(href,response.status))
        if response.status!= 200 and redo < 10:
            redo = redo + 1
            return await get_page(href=href,proxy=None, redo=redo)
        else:
            return await response.text()

async def get_sal_dict(table):
    salary_dict = {}
    salaries = []
    updated_dates = []
    rows = pq(table).children("tr")
    for i in rows:
        salaries.append(pq(pq(i).children("td")[1]).text())
        updated_dates.append(datetime.strptime(pq(pq(i).children("td")[3]).text(), '%B %d, %Y'))
    salary_dict['10'] = {'Salary':salaries[0], 'Updated Date':updated_dates[0]}
    salary_dict['25'] = {'Salary': salaries[1], 'Updated Date': updated_dates[1]}
    salary_dict['50'] = {'Salary': salaries[2], 'Updated Date': updated_dates[2]}
    salary_dict['75'] = {'Salary': salaries[3], 'Updated Date': updated_dates[3]}
    salary_dict['90'] = {'Salary': salaries[4], 'Updated Date': updated_dates[4]}
    return salary_dict

async def get_locs(pq_obj):
    loc_dict = {}
    location_ele = pq_obj("#sltmetro")
    loc_list = pq(location_ele).children()[1:]
    base_url = "https://www.salary.com"
    for i in loc_list:
        loc_dict['location_name'] = pq(i).text()
        loc_dict['loc_link'] = base_url + pq(i).attr("value")
    return loc_dict

async def get_salary_from_page(data_queue, sal_type):
    meta_data = await data_queue.get()
    hit_url = meta_data['data']['Link'] + "?view=table&type="+sal_type
    #print(hit_url)
    meta1 = meta_data['meta']
    meta2 = meta_data['data']
    page_html = await get_page(hit_url)
    pq_obj = pq(page_html)
    loc_dict = await get_locs(pq_obj)
    table = pq_obj("div#divtable").children("div.padding-left15.padding-right5").children("table").children("tbody")
    #print(table.text())
    salary_dict = await get_sal_dict(table)
    client = motor.motor_asyncio.AsyncIOMotorClient()
    harvests_db = client['salarydotcom']
    data_coll = harvests_db['salary_data_no_loc'+sal_type]
    loc_coll = harvests_db['consul3'+sal_type]
    data_dict = {"meta1":meta1, "meta2":meta2, "data":{"JobRole2": meta_data['data']['JobRole'], "salary_data":salary_dict}}
    logging.info(f'DB OP: Inserting data for {meta_data["data"]["JobRole"]}')
    await data_coll.find_one_and_update({'data.JobRole2':meta_data['data']['JobRole']},{"$set":data_dict}, upsert=True)
    loc_final_dict = {'meta2':meta_data, "Job Role":meta_data['data']['JobRole'], "Location Details":loc_dict}
    await loc_coll.find_one_and_update({"Job Role":meta_data['data']['JobRole']},{'$set':loc_final_dict}, upsert=True)
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
            task = asyncio.Task(get_salary_from_page(async_queue, sal_type))
            tasks.append(task)
        await asyncio.gather(*tasks)

def driver_sal(process_queue_size, data_queue, sal_type):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(make_tasks_and_exc(process_queue_size, data_queue, sal_type))

def get_salary(sal_type, no_processes):
    data_queue = get_data_q()
    logging.info(f'Master Queue Size: {(data_queue.qsize())}')
    process_queue_size = (data_queue.qsize() // no_processes) + 1
    with multiprocessing.Pool(no_processes) as p:
        logging.info(f'Initiating pool of {no_processes} worker processes')
        multi = [p.apply_async(driver_sal, (process_queue_size, data_queue,sal_type, )) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()

def get_data_q():
    client = MongoClient()
    harvests_db = client['salarydotcom']
    consul2_coll = harvests_db['consul2']
    data_queue = multiprocessing.Manager().Queue()
    res = consul2_coll.find({})
    for i in list(res):
        data_queue.put(i)
    return data_queue

if __name__=='__main__':
    PROCESSES = 16
    sal_type = 'base'
    get_salary(sal_type, PROCESSES)