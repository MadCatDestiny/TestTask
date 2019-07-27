import os,threading,json,re,logging,ftplib,sys

#Def function for thread
def worker(paths,ip,port,data):
    # Connect to server
    ftp = ftplib.FTP()
    cn = ftp.connect(ip,port)
    if cn.startswith('220'):
        logging.debug('Connected successful')
    else:
        logging.error('Connection error: '+ cn)
        sys.exit(1)

    cn = ftp.login(data['login'],data['password'])
    if cn.startswith('230'):
        logging.debug('Authorization successful')
    else:
        logging.error('Authorization error: ' + cn)
        sys.exit(1)

    #Download files
    for path in paths.items():
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
            logging.warning('Path does not exist: ' + path[1])
        except ConnectionRefusedError:
            logging.error('[WinError 10061] Подключение не установлено, т.к. конечный компьютер отверг запрос на подключение')
            sys.exit(1)

########################################################################################################################
#----------------------------------------------------------------------------------------------------------------------#
########################################################################################################################

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logging.debug('Start program')
#Creating json
path = 'inf.json'
data = {
           'ftp':'192.168.0.105',
           'port':21,
           'login':'hb1998',
           'password':'l0gpass',
           'max_conn':2,
            'paths':
                {
                "C:\\Users\\hb199\\PycharmProjects\\ftp_client\\F.txt": '/Blocks/F.txt',
                "C:\\Users\\hb199\\PycharmProjects\\ftp_client\\A.txt": '/Blocks/A.txt',
                "C:\\Users\\hb199\\PycharmProjects\\ftp_client\\G.txt": '/Blocks/G.txt',
                "C:\\Users\\hb199\\PycharmProjects\\ftp_client\\R.txt": '/Blocks/R.txt',
                "C:\\Users\\hb199\\PycharmProjects\\ftp_client\\S.txt": '/Blocks/S.txt'
                }
        }
with open (path,'w') as f:
    json.dump(data,f,indent=2)
logging.debug('json written successful')

#Parsing json
if os.path.exists(path):
    with open('inf.json','r') as f:
        data = json.load(f)
    logging.debug('Data loaded successful')
else:
    logging.error('File not found')
    sys.exit(1)


#Check data
if len(data) != 6:
    logging.error('Wrong data from json')
    sys.exit(1)
#Check ip
ip = data['ftp']
logging.debug('ip: ' + str(ip))
splited_ip = ip.split('.')
if splited_ip[0].isdecimal():
    for i in splited_ip:
        if int(i) > 255:
            logging.error('Wrong ip')
            sys.exit(1)
""" #or this  
rg = re.compile(r'(\d\d(\d)?\.\d\d(\d)?\.\d(\d\d)?\.\d\d(\d)?)')
print(rg.search(ip).group(1))
"""
#Check port
port = data['port']
logging.debug('port: ' + str(port))
if type(port) == type(int()):
    if  port > 0 :
        logging.debug('Correct port')
    else:
        logging.error('Wrong port (<= 0)')
        sys.exit(1)
else:
    logging.error('Wrong port (not int)')
    sys.exit(1)

#Check paths
paths = data['paths']
if type(paths) != type(dict()):
    logging.error('Wrong container for paths')
    sys.exit(1)
if len(paths) == 0:
    logging.error('Container is empty')
    sys.exit(1)
#Chek max_conn
max_conn = data['max_conn']
logging.debug('max_conn: ' + str(max_conn))
if type(max_conn) == type(int()):
    if  max_conn > 0 :
        logging.debug('Correct max_conn')
    else:
        logging.error('Wrong max_conn (<= 0)')
        sys.exit(1)
else:
    logging.error('Wrong max_conn (not int)')
    sys.exit(1)


if max_conn >= len(paths):
    for i in paths.items():
        # Download files in multithreading mode
        t = threading.Thread(target = worker,args=(i,ip,port,data,))
        t.start()
else: #Distribution of paths
    residue = len(paths)%max_conn
    for_one = len(paths)//max_conn
    paths_to_thread = []
    paths_count = 0
    paths_items = list(paths.items())
    for i in range(max_conn):
        paths_to_thread.append({})
        if i != max_conn-1: #Add in last thread the remaining paths
            for j in range(for_one):
                paths_to_thread[i].setdefault(paths_items[paths_count][0],paths_items[paths_count][1])
                paths_count+=1
        else:
            for j in range(for_one+residue):
                paths_to_thread[i].setdefault(paths_items[paths_count][0],paths_items[paths_count][1])
                paths_count+=1
    for i in range(max_conn):
        t = threading.Thread(target=worker, args=(paths_to_thread[i], ip, port, data,))
        t.start()


