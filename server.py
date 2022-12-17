import sys
from socket import *
from threading import Thread
import json
from datetime import datetime
import os


class ClientThread(Thread):
    def __init__(self, clientAddress, clientSocket):
        Thread.__init__(self)
        self.clientAddress = clientAddress
        self.clientSocket = clientSocket
        self.clientUDPPort = None
        self.clientAlive = False
        print("===== New connection created for: ", clientAddress)
        self.clientAlive = True
        self.client_usr = None

    def run(self):
        msg = ''
        # server initiate authentication
        self.process_login()
        print(f'{self.client_usr}; {self.clientAddress[0]}:{self.clientAddress[1]} logged in')

        # receive request from edge device
        while self.clientAlive:
            data = self.clientSocket.recv(1024)
            msg = json.loads(data.decode('utf-8'))

            # disconnect
            if msg == '':
                self.clientAlive = False
                self.clientSocket.close()
                print("===== the user disconnected - ", self.clientAddress)
                continue

            # logout
            elif msg['messagetype'] == 'OUT':
                print(f'> The edge device {self.client_usr} issued OUT command')
                self.process_logout()
                print(f'> {self.client_usr} {self.clientAddress[0]}:{self.clientAddress[1]} exited the edge network')
                self.clientAlive = False
                break

            elif msg['messagetype'] == 'UED':
                print(f'> The edge device {self.client_usr} issued UED command')
                upload_time = datetime.now()
                upload_time_s = upload_time.strftime('%-d %B %Y %H:%M:%S')
                self.receive_file(msg['file_id'], msg['file'], msg['data_amount'], upload_time_s)

            elif msg['messagetype'] == 'DTE':
                self.delete_file(msg['file_id'])

            elif msg['messagetype'] == 'SCS':
                print(f'> The edge device {self.client_usr} issued SCS command')
                self.compute(msg['file_id'], msg['operation'])

            elif msg['messagetype'] == 'AED':
                print(f'> The edge device {self.client_usr} issued AED command')
                self.active_edge_devices()

            else:
                print("[recv] " + msg)
                print("[send] Cannot understand this message")

    def process_login(self):
        message = 'user credentials request'
        print('[send] ' + message)
        self.clientSocket.send(message.encode())

        while True:
            data = self.clientSocket.recv(1024)
            cred = json.loads(data.decode('utf-8'))

            # safety check
            if cred['messagetype'] != 'credentials':
                self.clientAlive = False
                self.clientSocket.close()
                return

            print('[recv] credentials')
            usr = cred['username']
            pwd = cred['password']

            # check if client is still blocked
            if usr in blocked_usr:
                time_blocked = datetime.now() - blocked_usr[usr]
                if time_blocked.total_seconds() < block_duration:
                    self.clientSocket.send(bytes('bb', encoding='utf-8'))
                    continue
                else:
                    del blocked_usr[usr]

            # valid credentials, login
            elif usr in credentials and pwd == credentials[usr]:
                self.client_usr = usr
                self.clientSocket.send(bytes('ok', encoding='utf-8'))
                self.clientUDPPort = int(self.clientSocket.recv(1024).decode('utf-8'))

                # update active edge device list and edge-device-log.txt
                login_time = datetime.now()
                login_time_s = login_time.strftime('%-d %B %Y %H:%M:%S')
                active_edge_devices.append([login_time_s, usr, self.clientAddress, self.clientUDPPort])

                # Active edge device sequence number; timestamp; device name; IP address; UDP port
                # 1; 30 September 2022 10:31:13; supersmartwatch; 129.64.31.13; 5432
                with open('edge-device-log.txt', 'w') as edl:
                    for i in range(len(active_edge_devices)):
                        edl.write(f'{i}; {active_edge_devices[i][0]}; {active_edge_devices[i][1]}; {active_edge_devices[i][2]}; {active_edge_devices[i][3]}')
                return

            else:
                if usr not in credentials:  # invalid username
                    self.clientSocket.send(bytes('usr', encoding='utf-8'))

                elif usr not in no_failed_login:  # invalid password, start failed login count
                    no_failed_login[usr] = 1
                    self.clientSocket.send(bytes('pwd', encoding='utf-8'))

                else:  # invalid password, continue failed login count
                    no_failed_login[usr] += 1
                    # block if reach login limit
                    if no_failed_login[usr] == failed_login_limit:
                        self.clientSocket.send(bytes('b', encoding='utf-8'))
                    self.clientAlive = False
                    self.clientSocket.close()
                    print("the user disconnected - ", self.clientAddress)
                    break

    def process_logout(self):

        # UDP tell user recv thread to exit
        udp_socket = socket(AF_INET, SOCK_DGRAM)
        udp_socket.sendto('lo'.encode('utf-8'), (self.clientAddress[0], self.clientUDPPort))
        print('[send] UDP log out message')

        # TCP Inform client of successful logout
        self.clientSocket.send(bytes('so', encoding='utf-8'))
        print('[send] TCP send successful logout message')

        # Remove device logging out from list
        for i in range(len(active_edge_devices)):
            if active_edge_devices[i][1] == self.client_usr:
                del active_edge_devices[i]
                break
        # Update active edge devices log file
        with open('edge-device-log.txt', 'w') as edl:
            for i in range(len(active_edge_devices)):
                edl.write(f'{i+1}; {active_edge_devices[i][0]}; {active_edge_devices[i][1]}; {active_edge_devices[i][2][0]}; {active_edge_devices[i][3]}\n')

    def active_edge_devices(self):
        aed_d = dict()
        print(active_edge_devices)
        for d in active_edge_devices:
            if d[1] != self.client_usr:
                aed_d[d[1]] = (d[0], d[2], d[3])  # usr : (timestamp, IP addr, UDP port)
        self.clientSocket.send(bytes(json.dumps(aed_d), encoding='utf-8'))

    def receive_file(self, file_id, file, data_amount, upload_time):
        # save file
        with open(f'{self.client_usr}-{file_id}.txt', 'w') as rf:
            rf.write(file)

        # update file_id dictionary
        # int fileID : [str edgeDeviceName, str upload_timestamp, int dataAmount, bool deleted]
        file_ids[file_id] = [self.client_usr, upload_time, data_amount]

        # update file upload log
        with open('upload-log.txt', 'a') as ulog:
            ulog.write(f'{self.client_usr}; {upload_time}; {file_id}; {data_amount}\n')

        print(f'File {self.client_usr}-{file_id}.txt uploaded')

        # inform sender of successful upload
        print('[send] edge data upload successful')
        self.clientSocket.send(bytes('su', encoding='utf-8'))

    def delete_file(self, f_id):
        try:
            fname = f'{file_ids[f_id][0]}-{f_id}.txt'
            os.remove(fname)
            self.clientSocket.send(bytes('sd', encoding='utf-8'))  # successful delete
        except:
            print('File does not exist')
            self.clientSocket.send(bytes('nf', encoding='utf-8'))
            return

        # remove from fileID list and write to deletion log
        # edgeDeviceName; timestamp; fileID; dataAmount
        deleted_f = file_ids.pop(f_id)
        with open('deletion-log.txt', 'a') as dlog:
            dlog.write(f'{deleted_f[0]}; {deleted_f[1]}; {f_id}; {deleted_f[2]}\n')

        print(f'File with ID of {f_id} has been successfully removed from the central server')

    def compute(self, file_id, operation):
        try:
            fname = f'{file_ids[file_id][0]}-{file_id}.txt'
        except:
            print('File does not exist')
            self.clientSocket.send(bytes('nf', encoding='utf-8'))
            return

        with open(fname, 'r') as fh:
            tmp = fh.read().split()

        cal_inp = [int(e) for e in tmp]

        if operation == 'SUM':
            cal_out = sum(cal_inp)
        elif operation == 'AVERAGE':
            cal_out = sum(cal_inp)/len(cal_inp)
        elif operation == 'MAX':
            cal_out = max(cal_inp)
        elif operation == 'MIN':
            cal_out = min(cal_inp)
        else:
            print('Operation not supported')
            return

        print(f'Result is: {cal_out}')
        self.clientSocket.send(bytes(str(cal_out), encoding='utf-8'))


if len(sys.argv) != 3:
    print("\n===== Error usage, python3 server.py server_port no_of_allowed_failed_login ======\n")
    exit(0)
elif sys.argv[2].isdigit() is False:
    print("> No. of allowed failed login attempts must be integer from 1-5")
    print("\n===== Error usage, python3 server.py server_port no_of_allowed_failed_login ======\n")
    exit(0)

server_port = int(sys.argv[1])
failed_login_limit = int(sys.argv[2])
block_duration = 10
blocked_usr = {}
no_failed_login = {}
credentials = {}
active_edge_devices = []  # [login_time_s, usr, self.clientAddress, self.clientUDPPort]
file_ids = {}  # int fileID : [str edgeDeviceName; str upload_timestamp; int dataAmount; bool deleted]

with open('credentials.txt') as fhand:
    for line in fhand.readlines():
        usr_pwd = line.split(' ')
        credentials[usr_pwd[0]] = usr_pwd[1].strip()

server_socket = socket(AF_INET, SOCK_STREAM)
hostname = gethostname()
address = gethostbyname(hostname)
server_socket.bind((address, server_port))
server_socket.listen()

print("\n===== Server is running =====")
print("===== Waiting for connection request from clients...=====")

while True:
    client_socket, client_address = server_socket.accept()
    client_thread = ClientThread(client_address, client_socket)
    client_thread.start()

