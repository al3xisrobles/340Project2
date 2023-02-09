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
        self.closed = False
        self.ack = False
        self.fin = False

        executer = ThreadPoolExecutor(max_workers = 1)
        executer.submit(self.listener)

    def indiv_send(self, chunk: bytes, ack: bool, fin: bool, hash: bytes):
        if not ack:
            self.sequence_number += 1
        header = struct.pack('!I??16s', self.sequence_number, ack, fin, hash)

        if ack:
            print('Sending an ACK...')
            print()
            self.socket.sendto(header, (self.dst_ip, self.dst_port))

        
        else:
            sent = False
            self.ack = False
            packet = header + chunk

            # while we haven't recieved an ACK
            while not sent:
                t = 0
                print('\nSENDING CHUNK:', chunk)
                print('WITH SEQ NUM:', self.sequence_number)
                print('AND ACK: 0')
                print()
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))

                # haven't recieved an ack and still within the waiting time
                while not self.ack and t <= 25:
                    time.sleep(0.01)
                    t += 1

                # if self.ack is true, then we received an ack and dont want 
                # to send the packet again
                sent = self.ack







    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""

        MAX_PAYLOAD_LENGTH = 1472


        # Fake header to get header size
        header = struct.pack('!I??16s', self.sequence_number, self.ack, False, hashlib.md5(b"test").digest())

        # Separate packet into multiple packets of size MAX_PAYLOAD_LENGTH -
        # header since we're going to add a header to each later
        chunks = []
        for i in range(0, len(data_bytes), MAX_PAYLOAD_LENGTH - len(header)):
            chunks.append(data_bytes[i:i + MAX_PAYLOAD_LENGTH - len(header)])

        # Send each chunk at a time
        for chunk in chunks:

            # send chunk
            self.indiv_send(chunk, False, False, hashlib.md5(chunk).digest())


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
        
        self.indiv_send(b"FIN", False, True, hashlib.md5(b"FIN").digest())

        while not self.fin:
            time.sleep(0.01)
        
        time.sleep(2)

        self.closed = True
        self.socket.stoprecv()

        return

    def listener(self):
        last_num = 0

        while not self.closed: # a later hint will explain self.closed
            try:

                # Listen for packets
                data, addr = self.socket.recvfrom()

                # Unpack the packet
                header = data[:22]
                seq_num = struct.unpack('!I??16s', header)[0]
                ack = struct.unpack('!I??16s', header)[1]
                fin = struct.unpack('!I??16s', header)[2]
                hash = struct.unpack('!I??16s', header)[3]

                # If an ACK is received, tell the object to break out of the
                # while sleep loop
                if ack:
                    self.ack = ack

                    # if its an ack for the fin packet
                    if fin:
                        self.fin = True
                        print("recieved FIN")

                # If the incoming packet's ACK flag is not set
                if not ack:
                    payload = data[22:]
                    
                    # Checking hash
                    if hashlib.md5(payload).digest() != hash:
                        print("\nCORRUPT!")
                        continue
                    
                    if seq_num != last_num:
                        # Add data to buffer
                        self.receive_buffer[seq_num] = payload
                        last_num = seq_num

                        # Increment the seq num by 1 since it should increase by 1
                        # with each new packet received
                        self.sequence_number += 1
                    else:
                        print("\nAlready recieved")

                    print("\nRECEIVED A PACKET:")
                    print('SEQ NUM:', self.sequence_number)
                    print('ACK:', self.ack)
                    print('BUFFER:', str(list(self.receive_buffer.keys())))

                    # Send an ACK packet back
                    # self.ack = 1
                    self.indiv_send(data, True, fin, hash)

            except Exception as e:
                print("listener died!")
                print(e)
