#coding:utf-8
import uuid
import time
import grequests
import pymongo
import redis
try:
    import MySQLdb as sql
except:
    import mysql.connector as sql
import json
import random
import requests
from timeit import Timer

REDIS_URL = 'redis://:pwd@47.92.32.73:port/2'
redis_client=redis.from_url(REDIS_URL)


MONGO_URI='mongodb://name:pwd@47.92.32.73:port'
MONGO_DATABASE='mobai'
mongo_client=pymongo.MongoClient(MONGO_URI)
mongo_db=mongo_client[MONGO_DATABASE]
mongo_collection=mongo_db.mobaibeijing


MYSQL_HOST='47.92.32.73'
MYSQL_USER='name'
MYSQL_PASSWD='pwd'
MYSQL_DB='proxy'
MYSQL_PORT=3306
try:
    sql_connection=sql.connect(host=MYSQL_HOST,user=MYSQL_USER,password=MYSQL_PASSWD,database=MYSQL_DB,port=MYSQL_PORT)
except:
    sql_connection=sql.connect(MYSQL_HOST,MYSQL_USER,MYSQL_PASSWD,MYSQL_DB,MYSQL_PORT)
sql_cursor=sql_connection.cursor()
sql_cursor.execute('select * from mobaiproxy')
result_tuple=sql_cursor.fetchall()
print result_tuple
ips=[i[0] for i in result_tuple]


def exclude_same_request(func):
    def wrapper(*args,**kwargs):
        if kwargs.get('dont_filter',False):
            #print 'dont filter the failed request'
            return func(*args)
        if not redis_client.sismember('mobai:beijing:already_parse',args):
            redis_client.sadd('mobai:beijing:already_parse',args)
            return func(*args)
        else:
            print 'repeat'        
    return wrapper
    
'''
def exception_handler(request, exception):  #request是grequests的AsyncRequest实例
    print 'got exception ,rebuild request'
    proxy=request.kwargs['proxies']['https']
    try:
        ips.remove(proxy)   #有时 proxy不在ips里 奇怪
    except Exception,e:
        print e
    latitude=request.kwargs['data']['latitude']
    longitude=request.kwargs['data']['longitude']
    put_result_into_wait([(longitude,latitude)],dont_filter=True)
'''

@exclude_same_request
def constuct_request(longitude,latitude,dont_filter=False):
    url = "https://api.mobike.com/mobike-api/rent/nearbyBikesInfo.do"
    payload = {
        'cityCode':'010',
        'biketype':'1',
        'latitude':latitude,
        'scope':'500',
        'sign':str(uuid.uuid4()).replace('-',''),
        'client_id':'android',
        'longitude':longitude
    }
    proxies={
        "https":random.choice(ips)
    }
    headers = {
        'content-length': "130",
        'content-type': "application/x-www-form-urlencoded",
        'host': "api.mobike.com",
        'connection': "Keep-Alive",
        'accept-encoding': "gzip",
        'platform': "1",
        'eption': str(uuid.uuid4())[:5].replace('-',''),
        'citycode': "010",
        'os': "22",
        'lang': "zh",
        'version': "5.1.1",
        'uuid': str(uuid.uuid4()).replace('-',''),
        'time': str(int(time.time()*1000)),
        'User-Agent':None,
        'Accept':None
        }
    return grequests.post(url, data=payload, headers=headers,proxies=proxies)

'''
def recursive_request(coordinates):
    reqs=[constuct_request(latitude,longitude) for latitude,longitude in coordinates]
    reqs=[req for req in reqs if req] #去除reqs里边的None,exclude_same_request装饰过的constuct_request有时候会返回None
    ret=grequests.map(reqs,size=5,exception_handler=exception_handler,gtimeout=5)  #如果size不为None 则map会使用gevent.pool,协程池最大跑5个协程
    for i in ret:
        list_bikes=i.json()['object']
        mongo_collection.insert_many(list_bikes)
        new_coordinates=[(str(round(bike['distY'],6)),str(round(bike['distX'],6))) for bike in list_bikes]
        print new_coordinates
        recursive_request(new_coordinates)
        time.sleep(5)
'''
retry_coordinates=[]
def put_result_into_wait(coordinates,dont_filter=False):
    rqs=[]
    print 'still have %s ips' % len(ips)
    for i in coordinates:
        rq=constuct_request(i[0],i[1],dont_filter=dont_filter)
        if rq:
            rqs.append(rq)    
    if not rqs:return 
    ret=grequests.map(rqs,size=20,gtimeout=5)  #如果size不为None 则map会使用gevent.pool,协程池最大跑40个协程
    print ret
    for i in range(len(ret)):
        if not ret[i]: #处理response为None,重发request 
            proxy=rqs[i].kwargs['proxies']['https']
            try:
                ips.remove(proxy)   #有时 proxy不在ips里 奇怪
            except Exception,e:
                print e
            latitude=rqs[i].kwargs['data']['latitude']
            longitude=rqs[i].kwargs['data']['longitude']
            global retry_coordinates
            retry_coordinates.append((longitude,latitude))
            if len(retry_coordinates)==20:
                copy_retry_coordinates=retry_coordinates[:]
                retry_coordinates=[]
                print '处理20个None response，重发request'
                put_result_into_wait(copy_retry_coordinates,dont_filter=True)
        elif ret[i].status_code !=200:
            print 'request failed,remove this ip'
            proxy=ret[i].request.proxies['https']
            ips.remove(proxy)
            latitude=ret[i].request.kwargs['data']['latitude']
            longitude=ret[i].request.kwargs['data']['longitude']
            coordinates=[(longitude,latitude)]
            put_result_into_wait(coordinates,dont_filter=True)
        else:
            try:
                #print ret[i].json()
                list_bikes=ret[i].json()['object']
                print '500米内车辆数据'
                print list_bikes
                if len(list_bikes)==0:
                    body= ret[i].request.body
                    i=body.find('longitude=')+10
                    i_end=body.find('&',i)
                    j=body.find('latitude=')+9
                    j_end=body.find('&',j)
                    longitude=float(body[i:i_end])
                    latitude=float(body[j:j_end])
                    print longitude,latitude
                    print u'500米内没有查出任何车辆，移除500米内的待爬坐标%s个' % remove_coords_nearby(longitude,latitude)
                else:
                    mongo_collection.insert_many(list_bikes)  #协程超过20个以上 mongodb服务器可能会崩溃自动关闭
            except Exception,e:
                print e
#500米内没有查出任何车辆，移除500米内的待爬坐标,南京15w坐标，此法移除了6w+坐标 芜湖14w坐标，此法排除了12w+坐标,北京43w坐标,此法排除了30w坐标
def remove_coords_nearby(longitude,latitude): 
    x0=longitude-0.004
    y0=latitude-0.004
    coords=[]
    for i in range(5):
        for j in range(5):
            x=str(round(x0+i*0.002,6))
            y=str(round(y0+j*0.002,6))
            coords.append(json.dumps([x,y]))
    return redis_client.srem('mobai:beijing:wait',*coords)
def add_into_redis(list_bikes):
    new_coordinates=[[str(round(bike['distY'],6)),str(round(bike['distX'],6))] for bike in list_bikes]
    new_coordinates=[json.dumps(i) for i in new_coordinates]
    redis_client.sadd('mobai:beijing:wait',*new_coordinates)
def pop_from_redis(n):
    item=[]
    try:
        item=[json.loads(redis_client.spop('mobai:beijing:wait')) for i in range(n)]
        item=[i for i in item if i]
    except Exception,e:
        print e  #没有数据时 json.loads出错
     #去除None元素
    print 'wait队列里取出待爬数据%s' % item
    return item
    
def get_from_wait():
    item=pop_from_redis(20)
    while item:
        put_result_into_wait(item)
        item=pop_from_redis(20)
        time.sleep(0.1)
        
        
        
        
def get_amap_border(city):
    url = "http://restapi.amap.com/v3/config/district"
    params={
        'key':'39823d263c29318db1e377f58bacf9fc',
        'keywords': city,
        'subdistrict':'0',
        'showbiz':'false',
        'extensions':'all'
    }
    data=requests.get(url, params=params).json()
    polyline=data['districts'][0]['polyline']
    center=data['districts'][0]['center']
    
    if polyline.find('|')>0:
        raise ValueError('got |')
    #polyline=polyline.split('|')[1]
    #字符串有 | 将区域隔开，每个区域的第一个坐标和最后一个坐标一样，各坐标按顺时针顺序排列。南京的只有一个 | ，丢弃第一部分区域（很小），只取第二部分区域
    
    amap_border=[(float(i.split(',')[0]),float(i.split(',')[1])) for i in polyline.split(';')]
    amap_border=amap_border[1:]  #第一个坐标和最后一个坐标一样，丢弃一个
    return amap_border
def get_baidu_border(city):
    amap_border=get_amap_border(city)
    group=zip(*([iter(amap_border)]*100))  #分组 每组100个坐标,但是如果amap_border不是100的整数倍的话 会遗漏最后一组不满100个的数据
    missing=amap_border[len(group)*100:]  #遗漏的坐标
    group.append(missing)
    baidu_border=[]
    for g in group:  #g格式 ((119.028277, 32.427333), (119.025263, 32.424603), (119.024844, 32.421872)...)
        coords= [str(i[0])+','+str(i[1]) for i in g]
        coords=';'.join(coords)
        url='http://api.map.baidu.com/geoconv/v1/?'+'coords='+coords+'&from=1&to=5&ak=cmbvUH9k1yT3igwIHBSW6ni7tOEqjrEj'
        result=requests.get(url).json()['result'] #格式 [{u'y': 32.220189700587, u'x': 119.25346988925}, {u'y': 32.214209744895, u'x': 119.24799065033}...]
        newg=[(round(i['x'],6),round(i['y'],6)) for i in result]
        print newg
        for i in newg:
            baidu_border.append(i)
    f = open('dump-beijing.txt', 'wb')
    json.dump(baidu_border,f)
    f.close()
    return baidu_border
    

    
    
def create_coordinates_from_border():
    f = open('dump-beijing.txt', 'rb')
    border=json.load(f)
    f.close()
    max_latitude=max(border,key=lambda x:x[1])[1]
    min_latitude=min(border,key=lambda x:x[1])[1]
    max_longitude=max(border,key=lambda x:x[0])[0]
    min_longitude=min(border,key=lambda x:x[0])[0]
    print max_latitude,max_longitude,min_latitude,min_longitude
    length_x=int((max_longitude-min_longitude)/0.002)
    length_y=int((max_latitude-min_latitude)/0.002)
    print length_x
    print length_y
    result=[]
    for i in range(length_x):
        for j in range(length_y):
            x=round(min_longitude+0.002*i,6) #运算之后小数点不止6位 奇怪
            y=round(min_latitude+0.002*j,6)
            if is_in_polygon(x,y,border):
                result.append((x,y))
    f = open('coords-beijing.txt', 'wb')
    json.dump(result,f)
    f.close()
                
    
    
def is_in_polygon(x,y,border): #x -longitude,  y  -latitude 如果点刚好在多边形的边上 可能会返还false
    result=False
    length=len(border)
    j=length-1  #j是i的上一个点
    for i in range(length):
        Yi=border[i][1]
        Yj=border[j][1]
        Xi=border[i][0]
        Xj=border[i][0]
        if ((y>Yj and y<=Yi) or (y>Yi and y<=Yj)) and (x>=Xi or x>=Xj):
            if (Xi+(y-Yi)/(Yj-Yi)*(Xj-Xi))<x:
                result= not result
        j=i
    return result        

def add_into_redis_from_file(filename):
    f = open(filename, 'rb')
    data=json.load(f)
    f.close()
    new_coordinates=[[str(round(i[0],6)),str(round(i[1],6))] for i in data] #[x,y]原来的x和y是数字，转化为字符 方便redis入库
    new_coordinates=[json.dumps(i) for i in new_coordinates]
    return redis_client.sadd('mobai:beijing:wait',*new_coordinates) #返回成功添加的数据条数

if __name__=='__main__':
    #first 获取北京的边界坐标数组
    #get_baidu_border(u'北京') 
    #second 从边界坐标数组里生成间隔每200米的坐标点
    #create_coordinates_from_border()
    #third 将坐标点压入redis
    #add_into_redis_from_file("coords-beijing.txt")
    #fourth redis取坐标并查询单车数据
    #get_from_wait()