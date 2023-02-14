import struct
from concurrent.futures import ThreadPoolExecutor
import time
import hashlib
import threading
from threading import Lock

# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP

# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY


class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.sequence_number = 0
        self.receive_buffer = {}

        # Lock
        self.lock = Lock()

        # Receive buffer
        self.received_packets = []

        # Send buffer
        self.send_buffer = {}
        self.timeout_time = 0.25

        # Earliest packet's sequence number that has been sent
        # without receiving an ACK
        # self.lowest_seq = 1

        # Contains all ACKs received client-side. ACK gets deleted from
        # here when there is a match with lowest seq, as you have received an
        # ack for lowest seq. This ack gets removed from this list and lowest
        # seq gets incremented by 1
        # self.received_acks = []

        # Time that has passed before first ACK is received
        # self.time = 0

        # Timeout has occurred, so start sending from the start of
        # the send buffer
        # self.timeout = False

        # Buffer currently being cleared (you are still sending packets from
        # the buffer so you DO NOT want new packets being sent from test.py)
        # self.clearing_buffer = False

        # Currently sending a packet (for the timer thread)
        # self.sending = False

        # Connection is closed
        self.closed = False

        # ACK packet
        self.ack = False

        # FIN packet
        self.fin = False

        # Thread 2 (listener)
        self.executer = ThreadPoolExecutor(max_workers = 100)
        self.executer.submit(self.listener)

        # Thread 3 (timer)
        # executer2 = ThreadPoolExecutor(max_workers = 1)
        # executer2.submit(self.timer)

    def indiv_send(self, chunk: bytes, buffer: bool, ack: bool, fin: bool):

        # # If we are receiving data from the buffer, just send it
        # # it is already packed up how we want it,
        # # chunk is the whole packet, header and data and hash
        # if buffer:
        #     seq_num = struct.unpack('!I??', chunk[:6])[0]
        #     print('\nSENDING CHUNK FROM BUFFER')
        #     print('WITH SEQ NUM:', seq_num)
        #     self.socket.sendto(chunk, (self.dst_ip, self.dst_port))

        #     # add sent packet to send buffer
        #     self.send_buffer[seq_num] = chunk

        # # we are receiving data from send function (so from test.py)
        # # or we are receiving data from the listener, meaning its an ack
        # else:

        #     #chunk is seq number in the case of acks
        #     if ack:
        #         print("sending ACK SEQ NUM:", chunk, "\n")
        #         header = struct.pack('!I??', chunk, ack, fin)
        #         self.socket.sendto(header, (self.dst_ip, self.dst_port))

        #     # ABOVE KEEP THE SAME

        #     # this is new data, from sent function, from test.py
        #     else:
        #         # sent = False
        #         # self.ack = False

        #         self.sequence_number += 1
        #         header = struct.pack('!I??', self.sequence_number, ack, fin)

        #         # create the hash for corruption
        #         hash = hashlib.md5(header+chunk).digest()

        #         packet = header + chunk + hash

        #         ### STOP & WAIT ###
        #         # while not sent:
        #         #     t = 0
        #         #     print('\nSENDING CHUNK:', chunk)
        #         #     print('WITH SEQ NUM:', self.sequence_number)
        #         #     print('AND ACK: 0')
        #         #     print()
        #         #     self.socket.sendto(packet, (self.dst_ip, self.dst_port))

        #         #     # add sent packet to send buffer
        #         #     self.send_buffer[self.sequence_number] = packet

        #         #     # haven't recieved an ack and still within the waiting time
        #         #     while not self.ack and t <= 25:
        #         #         time.sleep(0.01)
        #         #         t += 1

        #         #     # if self.ack is true, then we received an ack and dont want
        #         #     # to send the packet again
        #         #     sent = self.ack

        #         print('SENDING CHUNK')
        #         print('WITH SEQ NUM:', self.sequence_number)
        #         print()
        #         self.socket.sendto(packet, (self.dst_ip, self.dst_port))

        #         # add sent packet to send buffer
        #         self.send_buffer[self.sequence_number] = packet



        ## SELECTIVE REPEAT

        #chunk is seq number in the case of acks
        if ack:
            print("sending ACK SEQ NUM:", chunk, "\n")
            header = struct.pack('!I??', chunk, ack, fin)
            self.socket.sendto(header, (self.dst_ip, self.dst_port))

        # actual data
        else:
            self.sequence_number += 1
            header = struct.pack('!I??', self.sequence_number, ack, fin)

            # create the hash for corruption
            hash = hashlib.md5(header+chunk).digest()

            packet = header + chunk + hash
            
            # print('SENDING CHUNK')
            # print('WITH SEQ NUM:', self.sequence_number)
            # print()

            # self.socket.sendto(packet, (self.dst_ip, self.dst_port))

            # add sent packet to send buffer
            # I THINK WE CAN JUST MAKE THIS A LIST
            self.send_buffer[self.sequence_number] = "test"

            self.executer.submit(self.func_for_thread, packet)
            # threading.Thread(target = self.func_for_thread, args=(packet,))
            # self.send_buffer[self.sequence_number].run()


    def clear_buffer(self, buffer: dict):

        # set this to true so we don't keep sending new packets from send function, test.py
        self.clearing_buffer = True

        self.time = 0
        self.timeout = False

        # clear the acks we have already received
        self.received_acks.clear()

        # for i in range(self.lowest_seq, self.sequence_number):
        #     self.indiv_send(self.send_buffer[i], True, False, False, hashlib.md5(self.send_buffer[i]).digest())

        # clear the buffer as long as we haven't timed out
        i = self.lowest_seq
        while not self.timeout and i < self.sequence_number + 1:
            print("SEND BUFFER KEYS: "+ str(list(self.send_buffer.keys())))
            self.indiv_send(self.send_buffer[i], True, False, False)
            i += 1

        self.clearing_buffer = False

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""

        MAX_PAYLOAD_LENGTH = 1472
        self.sending = True

        # Fake header to get header size
        header = struct.pack('!I??', self.sequence_number, False, False)

        # Separate packet into multiple packets of size MAX_PAYLOAD_LENGTH -
        # header since we're going to add a header to each later
        chunks = []
        for i in range(0, len(data_bytes), MAX_PAYLOAD_LENGTH - len(header)):
            chunks.append(data_bytes[i:i + MAX_PAYLOAD_LENGTH - len(header)])

        # Send each chunk at a time
        for chunk in chunks:

            # while self.clearing_buffer:
            #     time.sleep(0.1)

            # send chunk
            self.indiv_send(chunk, buffer=False, ack=False, fin=False)

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""

        while True:

            if not self.receive_buffer:
                continue

            if self.sequence_number == max(self.receive_buffer.keys()):
                values = sorted(self.receive_buffer.items(), key=lambda x: x[0])
                values = [value[1] for value in values]
                data = b''.join(values)
                self.receive_buffer.clear()

                return data

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""

        # Wait for all packets to be sent
        while self.send_buffer:
            time.sleep(0.1)

        # Send FIN packet
        self.sending = True
        self.indiv_send(b'', False, False, True)
        while not self.fin:
            time.sleep(0.01)
        self.sending = False

        # Sleep for 2 seconds
        time.sleep(2)

        # Stop the listener and close socket
        self.closed = True
        self.socket.stoprecv()

        return

    def listener(self):
        # last_num = 0

        while not self.closed:
            try:

                # Listen for packets
                data, addr = self.socket.recvfrom()

                # Unpack the packet
                header = data[:6]
                seq_num = struct.unpack('!I??', header)[0]
                ack = struct.unpack('!I??', header)[1]
                fin = struct.unpack('!I??', header)[2]

                # If an ACK is received
                if ack:
                    # self.ack = ack

                    # if seq_num >= self.lowest_seq:
                    #     self.received_acks.append(seq_num)

                    # print("")
                    # print("RECEIVED ACKS:", str(self.received_acks))
                    # print("LOWEST SEQ NUM:", self.lowest_seq)

                    # Acknowledge that
                    # while self.lowest_seq in self.received_acks:
                    #     print("Removing seq num", str(self.lowest_seq) + '...')
                    #     self.time = 0
                    #     del self.send_buffer[self.lowest_seq]
                    #     self.received_acks.remove(self.lowest_seq)
                    #     self.lowest_seq += 1

                    if seq_num in self.send_buffer.keys():
                        del self.send_buffer[seq_num]

                    # if its an ack for the fin packet
                    if fin:
                        self.fin = True
                        print("received FIN")

                # If the incoming packet's ACK flag is not set
                else:

                    # self.sending = False
                    hash = data[-16:]

                    payload = data[:-16]

                    # Checking hash
                    if hashlib.md5(payload).digest() != hash:
                        print("\nCORRUPT!")
                        continue

                    # if seq_num not in self.receive_buffer.keys() and seq_num != last_num:

                    if seq_num not in self.received_packets:

                        # Add data to buffer
                        self.receive_buffer[seq_num] = payload[6:]
                        self.received_packets.append(seq_num)
                        # last_num = seq_num

                        # Increment the seq num by 1 since it should increase by 1
                        # with each new packet received
                        self.sequence_number += 1

                        print("\nRECEIVED A PACKET:", payload[6:])
                        print('SEQ NUM:', seq_num)
                        # print('ACK:', self.ack)
                        print('RECEIVE BUFFER:', str(list(self.receive_buffer.keys())))
                    else:
                        print("\nAlready recieved:", seq_num)

                    # Send an ACK packet back
                    # self.ack = 1
                    self.indiv_send(seq_num, buffer=False, ack=True, fin=fin)

            except Exception as e:
                print("listener died!")
                print(e)

    # def timer(self):

    #     while not self.closed:

    #         try:
    #             while self.sending:
    #                 while self.time <= 0.15:
    #                     time.sleep(0.01)
    #                     self.time += 0.01

    #                 print('\nTimeout!\n')
    #                 self.timeout = True
    #                 self.clear_buffer(self.send_buffer)

    #         except Exception as e:
    #             print("timer died!")
    #             print(e)


    def func_for_thread(self, packet):
        while not self.closed:
            try:

                #get seq num of the packet you want to send (for printing purposes)
                seq_num = struct.unpack('!I??', packet[:6])[0]
                acked = False

                # while you have not received an ack
                while not acked:
                    t = 0
                    print('\nSENDING CHUNK:', packet)
                    print('WITH SEQ NUM:', seq_num)
                    print()

                    #send the packet
                    self.socket.sendto(packet, (self.dst_ip, self.dst_port))

                    #wait 0.25 seconds
                    while t <= self.timeout_time:
                        time.sleep(0.01)
                        t += 0.01

                    # if seq num not in send buffer that means it has been acked
                    if seq_num not in self.send_buffer.keys():
                        acked = True
                    
            except Exception as e:
                print("thread for packet died!")
                print(e)