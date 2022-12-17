import sys
import json
import time
from threading import Thread
from socket import *
from select import *
from random import seed
from random import randint

class ReceiverThread(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.receive_socket = socket(AF_INET, SOCK_DGRAM)

    def run(self):
        # set up UDP port for receiving files from other edge devices
        self.receive_socket.bind((address, udp_port))
        global device_active
        device_active = True

        while True:
            if device_active is False:
                self.receive_socket.close()
                break

            msg, sender = self.receive_socket.recvfrom(1024)
            fn = msg.decode('utf-8')

            # not filename but message from server to let this thread know to exit
            if fn == 'lo':
                break

            vf = open(fn, 'wb')
            while True:
                ready = select([self.receive_socket], [], [], 3)
                if ready[0]:
                    data, addr = self.receive_socket.recvfrom(4096)
                    if addr == sender:
                        vf.write(data)
                else:
                    print(f'\n>{fn} download completed')
                    vf.close()
                    break


class SenderThread(Thread):
    def __init__(self, receivingUsr, fileName):
        Thread.__init__(self)
        self.send_socket = socket(AF_INET, SOCK_DGRAM)
        self.recv_usr = receivingUsr
        self.recv_ip = active_edge_devices[receivingUsr][1][0]
        self.recv_udp = active_edge_devices[receivingUsr][2]
        self.file_name = fileName
        self.recv_file_name = f'{usr}_{fileName}'

    def run(self):
        self.send_socket.sendto(self.recv_file_name.encode('utf-8'), (self.recv_ip, self.recv_udp))
        print(f'\n>Sending {self.file_name} ...')
        vf = open(self.file_name, 'rb')
        data = vf.read(4096)
        while data:
            if self.send_socket.sendto(data, (self.recv_ip, self.recv_udp)):
                data = vf.read(4096)
                time.sleep(0.02)  # Give receiver a bit time to save

        print(f'\n>{self.file_name} transfer completed')
        vf.close()
        self.send_socket.close()


def login():
    global usr
    global pwd
    usr = input('> Username: ')
    pwd = input('> Password: ')
    while True:
        credential = {'messagetype': 'credentials', 'username': usr, 'password': pwd}
        client_socket.send(bytes(json.dumps(credential), 'utf-8'))
        response = client_socket.recv(1024).decode('utf-8')
        if response == 'ok':
            client_socket.send(bytes(str(udp_port), 'utf-8'))
            print('\n> Welcome!')
            return
        elif response == 'usr':
            print('> Invalid Username. Please try again')
            usr = input('> Username: ')
            pwd = input('> Password: ')
        elif response == 'pwd':
            print('> Invalid Password. Please try again')
            pwd = input('> Password: ')
        elif response == 'b':
            print('> Invalid Password. Your account has been blocked. Please try again later')
            exit()
        elif response == 'bb':
            print('> Your account is blocked due to multiple authentication failures. Please try again later')
            exit()


def exit_edge_network():
    msg = {
        'messagetype': 'OUT'
    }
    client_socket.send(bytes(json.dumps(msg), 'utf-8'))
    response = client_socket.recv(1024).decode('utf-8')
    if response == 'so':
        client_socket.close()
        global device_active
        device_active = False
        print('> Goodbye.')
        exit()
    else:
        print('> Logout unsuccessful, please try again')


def edge_data_gen(file_id, data_amount):
    print(f'> The edge device is generating {data_amount} data samples...')

    with open(f'{usr}-{file_id}.txt', 'w') as fh:
        seed(1)
        for _ in range(data_amount):
            value = randint(0, 10)
            fh.write(f'{value}\n')

    # update file data amount dictionary
    file_data_amount[file_id] = data_amount

    print(f'\n> Data generation done, {data_amount} data samples have been generated and stored in the file {usr}-{file_id}.txt')


def upload_edge_data(file_id):
    try:
        with open(f'{usr}-{file_id}.txt', 'r') as edf:
            data = edf.read()
    except:
        print('> The file to be uploaded does not exist')
        return

    msg = {
        'messagetype': 'UED',
        'file_id': file_id,
        'file': data,
        'data_amount': file_data_amount[file_id]
    }

    client_socket.send(bytes(json.dumps(msg), 'utf-8'))
    response = client_socket.recv(1024).decode('utf-8')
    if response == 'su':
        print(f'> Data file with ID of {file_id} has been uploaded to server')
    else:
        print('> Upload unsuccessful, please try again')


def server_compute(file_id, operation):
    msg = {
        'messagetype': 'SCS',
        'file_id': file_id,
        'operation': operation
    }
    client_socket.send(bytes(json.dumps(msg), 'utf-8'))
    result = client_socket.recv(1024).decode()
    if result == 'nf':
        print('> File not found')
    else:
        print(f'> Result is: {result}')


def delete_data(file_id):
    msg = {
        'messagetype': 'DTE',
        'file_id': file_id
    }
    client_socket.send(bytes(json.dumps(msg), 'utf-8'))
    response = client_socket.recv(1024).decode()
    if response == 'nf':
        print('> File not found')
    else:
        print(f'> file with ID of {file_id} has been successfully removed from the central server')


def check_active_edge_devices():
    msg = {'messagetype': 'AED'}
    client_socket.send(bytes(json.dumps(msg), 'utf-8'))
    global active_edge_devices
    data = client_socket.recv(1024)
    active_edge_devices = json.loads(data.decode('utf-8'))
    if len(active_edge_devices) < 1:
        print('> No other active edge devices')
    else:
        for k, v in active_edge_devices.items():
            print(f'> {k}, active since {v[0]}, {v[1][0]}:{v[2]}')


if len(sys.argv) != 4:
    print("\n===== Error usage, python3 client.py SERVER_IP SERVER_PORT UDP_PORT======\n")
    exit(0)
server_host = sys.argv[1]
server_port = int(sys.argv[2])
udp_port = int(sys.argv[3])
hostname = gethostname()
address = gethostbyname(hostname)
usr = None
pwd = None
device_active = False
active_edge_devices = {}
file_data_amount = {}

# define a socket for the client side, it would be used to communicate with the server
client_socket = socket(AF_INET, SOCK_STREAM)

# build connection with the server and send message to it
client_socket.connect((server_host, server_port))
connection_response = client_socket.recv(1024).decode('utf-8')
if connection_response == 'user credentials request':
    login()
print('> Successfully login to the edge network')

# start thread for P2P receiving
receiver_thread = ReceiverThread()
receiver_thread.start()

while True:
    cmd = input('> Enter one of the following commands (EDG, UED, SCS, DTE, AED, UVF, OUT): ').split()

    if len(cmd) < 1:
        print('> Please try again')
        continue

    elif cmd[0] == 'EDG':
        # check for missing args
        if len(cmd) < 3:
            print('> fileID and/or data_amount is missing')

        # check if arguments are integers
        elif (cmd[1].isdigit() is False) or (cmd[2].isdigit() is False):
            print('> The fileID or data_amount are not integers, you need to specify the parameter as integers”.')

        else:
            f_id = int(cmd[1])
            dat_a = int(cmd[2])
            edge_data_gen(f_id, dat_a)

    elif cmd[0] == 'UED':
        if len(cmd) < 2:
            print('> fileID is needed to upload the data')
        elif cmd[1].isdigit() is False:
            print('> The fileID is not an integer, you need to specify the parameter as integer”.')
        else:
            f_id = int(cmd[1])
            # check if file exist
            if f_id not in file_data_amount:
                print('> The file to be uploaded does not exist')
            else:
                upload_edge_data(f_id)

    elif cmd[0] == 'SCS':
        if len(cmd) < 3:
            print('> fileID and/or operation is missing')
        elif cmd[1].isdigit() is False:
            print('> The fileID is not an integer, you need to specify the parameter as integer”.')
        elif cmd[2] not in {'SUM', 'AVERAGE', 'MAX', 'MIN'}:
            print('> Operation not supported')
        else:
            f_id = int(cmd[1])
            op = cmd[2]
            server_compute(f_id, op)

    elif cmd[0] == 'DTE':
        if len(cmd) < 2:
            print('> fileID is needed to upload the data')
        elif cmd[1].isdigit() is False:
            print('> The fileID is not an integer, you need to specify the parameter as integer”.')
        else:
            f_id = int(cmd[1])
            delete_data(f_id)

    elif cmd[0] == 'AED':
        check_active_edge_devices()

    elif cmd[0] == 'UVF':
        receiving_usr = cmd[1]
        file_name = cmd[2]
        check_active_edge_devices()
        # check if receiving user is online
        if receiving_usr not in active_edge_devices:
            print(f'> {receiving_usr} is offline')
            continue
        # start thread for uploading file
        sender_thread = SenderThread(receiving_usr, file_name)
        sender_thread.start()

    elif cmd[0] == 'OUT':
        exit_edge_network()
    else:
        print('> Command not recognized, please try again')


