import struct
from concurrent.futures import ThreadPoolExecutor
import time

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
        self.ack = 0

        executer = ThreadPoolExecutor(max_workers = 1)
        executer.submit(self.listener)

    def indiv_send(self, chunk: bytes, ack: bool):
        if not ack:
            self.sequence_number += 1
        header = struct.pack('!II', self.sequence_number, int(ack))

        if ack:
            print('Sending an ACK...')
            print()
            self.socket.sendto(header, (self.dst_ip, self.dst_port))

        
        else:
            sent = False
            self.ack = 0
            packet = header + chunk
            while not sent:
                t = 0
                print('\nSENDING CHUNK:', chunk)
                print('WITH SEQ NUM:', self.sequence_number)
                print('AND ACK: 0')
                print()
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                while not self.ack and t <= 25:
                    time.sleep(0.01)
                    t += 1

                sent = self.ack







    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""

        MAX_PAYLOAD_LENGTH = 1472


        # Fake header to get header size
        header = struct.pack('!II', self.sequence_number, self.ack)

        # Separate packet into multiple packets of size MAX_PAYLOAD_LENGTH -
        # header since we're going to add a header to each later
        chunks = []
        for i in range(0, len(data_bytes), MAX_PAYLOAD_LENGTH - len(header)):
            chunks.append(data_bytes[i:i + MAX_PAYLOAD_LENGTH - len(header)])

        # Send each chunk at a time
        for chunk in chunks:

            # send chunk
            self.indiv_send(chunk, False)


            # self.sequence_number += 1

            # header = struct.pack('!II', self.sequence_number, self.ack)

            # if self.ack:
            #     self.sequence_number -= 1

            # # add ack header

            # # If ACK, only send header
            # if self.ack:
            #     print('Sending an ACK...')
            #     print()
            #     packet = header
            # else:
            #     packet = header + chunk
            #     print('\nSENDING CHUNK:', chunk)
            #     print('WITH SEQ NUM:', self.sequence_number)
            #     print('AND SELF ACK:', self.ack)
            #     print()

            # # Send packet back to sender
            # self.socket.sendto(packet, (self.dst_ip, self.dst_port))

            # # Stop and wait until ACK is received
            # while not self.ack:
            #     time.sleep(0.01)

            # # Since an ACK was received, the
            # self.ack = 0

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
        self.closed = True
        self.socket.stoprecv()

    def listener(self):

        while not self.closed: # a later hint will explain self.closed
            try:

                # Listen for packets
                data, addr = self.socket.recvfrom()

                # Unpack the packet
                header = data[:8]
                seq_num = struct.unpack('!II', header)[0]
                ack = struct.unpack('!II', header)[1]

                # If an ACK is received, tell the object to break out of the
                # while sleep loop
                if ack:
                    self.ack = ack

                # If the incoming packet's ACK flag is not set
                if not ack:

                    # Add data to buffer
                    payload = data[8:]
                    self.receive_buffer[seq_num] = payload

                    # Increment the seq num by 1 since it should increase by 1
                    # with each new packet received
                    self.sequence_number += 1

                    print("\nRECEIVED A PACKET:")
                    print('SEQ NUM:', self.sequence_number)
                    print('ACK:', self.ack)
                    print('BUFFER:', str(list(self.receive_buffer.keys())))

                    # Send an ACK packet back
                    # self.ack = 1
                    self.indiv_send(data, True)

            except Exception as e:
                print("listener died!")
                print(e)
