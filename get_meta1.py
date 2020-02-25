from pyquery import PyQuery as pq
import aiohttp
import asyncio
import json
import motor.motor_asyncio
import time
import logging
logging.basicConfig(filename = 'jt_log.txt', level = logging.DEBUG, filemode = 'w')
from bson.json_util import loads
import multiprocessing
import time


async def get_json(payload, href= '', proxy=None,redo=0,request_type='POST'):
    async with aiohttp.ClientSession() as client:
        logging.info('Hitting API Url : {0}'.format(href))
        response = await client.request('{}'.format(request_type), href, proxy=proxy, data = payload)
        logging.info('Status for {} : {}'.format(href,response.status))
        if response.status!= 200 and redo < 10:
            redo = redo + 1
            return await get_json(payload, href=href,proxy=None, redo=redo)
        else:
            return await response.text()#.json(content_type='None')

async def get_job_titles(query_q):
    qstr = await query_q.get()
    payload = {"strJobKeyword":qstr}
    #print(payload)
    url = 'https://www.salary.com/research/salary/JobSalary/SalAjxGetJobsByKeyword'
    json_res = await get_json(payload, url )
    if(not json_res):
        logging.warning(f'No results from {url}, payload :{payload}')
        return
    json_sp = loads(json_res)
    json_sp = json.loads(json_sp)
    client = motor.motor_asyncio.AsyncIOMotorClient()
    harvests_db = client['salarydotcom']
    consul1_coll = harvests_db['consul1']
    for j in json_sp:
        logging.info(f'DB Op:Inserting Job w/ Jobcode:{j["JobCode"]}')
        await consul1_coll.find_one_and_update({'JobCode':j['JobCode']}, {'$set':j}, upsert=True)
        #print(j['JobCode'])


async def asyncio_execute(process_queue_size, full_q):
    query_q = asyncio.Queue()
    for i in range(process_queue_size):
        if(not full_q.empty()):
            await query_q.put(full_q.get())
    logging.info(f'Per process queue size:{(query_q.qsize())}')
    tasks = []
    div_factor = 30
    times = query_q.qsize() // div_factor
    for _ in range(div_factor+1):
        await asyncio.sleep(0.2)
        for i in range(times):
            task = asyncio.Task(get_job_titles(query_q))
            tasks.append(task)
        logging.info(f'Initiating {times} batch tasks')
        await asyncio.gather(*tasks)

def gen_query_string(query_q):
    al_list = [chr(x) for x in range(ord('a'), ord('z') + 1)]
    for i in al_list:
        for j in al_list:
            for k in al_list:
                query_q.put(i+j+k)
    return query_q

def kw_driver(process_queue_size, query_q):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio_execute(process_queue_size, query_q))


def gen_query_and_exec(no_processes):
    query_q = multiprocessing.Manager().Queue()
    query_q = gen_query_string(query_q)
    logging.info(f'Query Queue Size :{(query_q.qsize())}')
    process_queue_size = (query_q.qsize()//no_processes) + 1
    with multiprocessing.Pool(no_processes) as p:
        logging.info('Initiating multi-pool workers')
        multi = [p.apply_async(kw_driver, (process_queue_size, query_q, )) for i in range(no_processes)]
        # clean up
        p.close()
        p.join()
    #await asyncio_execute(query_q)

if __name__=='__main__':
    PROCESSES = 16
    start = time.time()
    gen_query_and_exec(PROCESSES)
    end = time.time()
    print(f'Execution Time:{str(end-start)} seconds')