import struct
from concurrent.futures import ThreadPoolExecutor
import time
import hashlib

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
        
        #for the reciever
        self.received_packets = []

        # for the sender
        self.send_buffer = {}
        self.lowest_seq = 1
        self.received_acks = []
        self.timeout = False
        self.time = 0
        self.clearing_buffer = False
        self.sending = False

        self.closed = False
        self.ack = False
        self.fin = False

        executer = ThreadPoolExecutor(max_workers = 1)
        executer.submit(self.listener)

        executer2 = ThreadPoolExecutor(max_workers = 1)
        executer2.submit(self.timer)

    def indiv_send(self, chunk: bytes, buffer: bool, ack: bool, fin: bool):

        # If we are receiving data from the buffer, just send it
        # it is already packed up how we want it, 
        # chunk is the whole packet, header and data and hash
        if buffer:
            print('\nSENDING CHUNK FROM BUFFER:', chunk[6:])
            print('WITH SEQ NUM:',struct.unpack('!I??', chunk[:6])[0])
            self.socket.sendto(chunk, (self.dst_ip, self.dst_port))

            # add sent packet to send buffer
            # not sure if we need to do this again, depends on where data corruption is happening
            self.send_buffer[struct.unpack('!I??', chunk[:6])[0]] = chunk


        # we are receiving data from send function (so from test.py)
        # or we are receiving data from the listener, meaning its an ack
        else:
            
            #chunk is seq number in the case of acks
            if ack:
                print("Sending ACK SEQ NUM:", chunk)
                print()
                header = struct.pack('!I??', chunk, ack, fin)
                self.socket.sendto(header, (self.dst_ip, self.dst_port))

            
            # ABOVE KEEP THE SAME

            # this is new data, from sent function, from test.py
            else:
                # sent = False
                # self.ack = False

                self.sequence_number += 1
                header = struct.pack('!I??', self.sequence_number, ack, fin)

                #create the hash
                hash = hashlib.md5(header+chunk).digest()

                packet = header + chunk + hash

                # # while we haven't recieved an ACK
                # while not sent:
                #     t = 0
                #     print('\nSENDING CHUNK:', chunk)
                #     print('WITH SEQ NUM:', self.sequence_number)
                #     print('AND ACK: 0')
                #     print()
                #     self.socket.sendto(packet, (self.dst_ip, self.dst_port))

                #     # add sent packet to send buffer
                #     self.send_buffer[self.sequence_number] = packet

                #     # haven't recieved an ack and still within the waiting time
                #     while not self.ack and t <= 25:
                #         time.sleep(0.01)
                #         t += 1

                #     # if self.ack is true, then we received an ack and dont want 
                #     # to send the packet again
                #     sent = self.ack

                print('\nSENDING CHUNK:', chunk)
                print('WITH SEQ NUM:', self.sequence_number)
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))

                # add sent packet to send buffer
                self.send_buffer[self.sequence_number] = packet

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

            while self.clearing_buffer:
                time.sleep(0.1)

            # send chunk
            self.indiv_send(chunk, False, False, False)


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
        
        self.indiv_send(b'', False, False, True)

        while not self.fin:
            time.sleep(0.01)
        
        time.sleep(2)

        self.closed = True
        self.socket.stoprecv()

        return

    def listener(self):
        # last_num = 0

        while not self.closed: # a later hint will explain self.closed
            try:

                # Listen for packets
                data, addr = self.socket.recvfrom()

                # Unpack the packet
                header = data[:6]
                seq_num = struct.unpack('!I??', header)[0]
                ack = struct.unpack('!I??', header)[1]
                fin = struct.unpack('!I??', header)[2]

                # If an ACK is received, tell the object to break out of the
                # while sleep loop
                if ack:
                    # self.ack = ack

                    self.received_acks.append(seq_num)

                    print("")
                    print("RECEIVED ACKS:",str(self.received_acks))

                    while self.lowest_seq in self.received_acks:
                        self.time = 0
                        del self.send_buffer[self.lowest_seq]
                        self.received_acks.remove(self.lowest_seq)
                        self.lowest_seq += 1

                    # if its an ack for the fin packet
                    if fin:
                        self.fin = True
                        print("received FIN")

                # If the incoming packet's ACK flag is not set
                if not ack:
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

                        print("\nRECEIVED A PACKET:")
                        print('SEQ NUM:', seq_num)
                        # print('ACK:', self.ack)
                        print('RECEIVE BUFFER:', str(list(self.receive_buffer.keys())))
                    else:
                        print("\nAlready recieved:", seq_num)


                    # Send an ACK packet back
                    # self.ack = 1
                    self.indiv_send(seq_num, False, True, fin)

            except Exception as e:
                print("listener died!")
                # print(e)


    def timer(self):

        while not self.closed:

            try:
                while self.sending:
                    while self.time <= 0.5:
                        time.sleep(0.01)
                        self.time += 0.01
                    
                    self.timeout = True
                    self.clear_buffer(self.send_buffer)

            except Exception as e:
                print("listener timer died!")
                # print(e)