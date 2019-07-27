import os,threading,json,re,logging,ftplib
logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logging.debug('Start program')

#Creating json
path = 'inf.json'
data = ['192.168.0.105',21,'hb1998','l0gpass',2,{"C:\\Users\\hb199\\PycharmProjects\\ftp_client\\F.txt":'/Blocks/F.txt',"C:\\Users\\hb199\\PycharmProjects\\ftp_client\\S.txt":'/Blocks/S.txt'}]
with open (path,'w') as f:
    json.dump(data,f,indent=2)
logging.debug('json written successful')

#Parsing json
if os.path.exists(path):
    with open('inf.json','r') as f:
        data = json.load(f)
    logging.debug('data loaded successful')
else:
    logging.error('file not found')

#Check data
if len(data) != 6:
    logging.error('Wrong data from json')
ip = data[0]
logging.debug('ip: ' + str(ip))
splited_ip = ip.split('.')
for i in splited_ip:
    if int(i) > 255:
        logging.error('Wrong ip')
""" #or this  
rg = re.compile(r'(\d\d(\d)?\.\d\d(\d)?\.\d(\d\d)?\.\d\d(\d)?)')
print(rg.search(ip).group(1))
"""

port = data[1]
logging.debug('port: ' + str(port))
if type(port) == type(int()):
    if  port > 0 :
        logging.debug('Correct port')
    else:
        logging.error('Wrong port (<= 0)')
else:
    logging.error('Wrong port (not int)')

paths = data[5]
if type(paths) != type(dict()):
    logging.error('Wrong container for paths')
if len(paths) == 0:
    logging.error('Container is empty')

#Def function for thread
def worker(path,ip,port,data):
    # Connect to server
    ftp = ftplib.FTP()
    cn = ftp.connect(ip,port)
    if cn.startswith('220'):
        logging.debug('Connected successful')
    else:
        logging.error('Connection error: '+ cn)

    cn = ftp.login(data[2],data[3])
    if cn.startswith('230'):
        logging.debug('Authorization successful')
    else:
        logging.error('Authorization error: ' + cn)


    #Download files
    try:
        lock = threading.Lock()
        lock.acquire()
        try:
            if not os.path.exists(path[0]):
                logging.error('File not found')
                return
            with open(path[0], 'rb') as f:
                res = ftp.storbinary('STOR ' + path[1], f)
                if res.startswith('226'):
                    logging.debug('Transfer complete: ' + path[0] + ' -> ' + path[1])
        finally:
            lock.release()
    except ftplib.error_perm:
        logging.error('Path does not exist')
    except ConnectionRefusedError:
        logging.error('[WinError 10061] Подключение не установлено, т.к. конечный компьютер отверг запрос на подключение')

for i in paths.items():
    # Download files in multithreading mode
    t = threading.Thread(target = worker,args=(i,ip,port,data,))
    t.start()
    #[WinError 10061] Подключение не установлено, т.к. конечный компьютер отверг запрос на подключение"""
    #worker(i,ip,port,data)

