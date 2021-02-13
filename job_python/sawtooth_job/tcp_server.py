import socket
import select
 
class TcpServer:
    def __init__(self, port):
        self.port = port
        self.srvsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.srvsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srvsock.bind(('136.186.108.248', port))
        self.srvsock.listen(10)
        self.descripors = [self.srvsock]
        print("Server started!")
 
    def run(self):
        while True:
            (sread, swrite, sexc) = select.select(self.descripors, [], [])
            for sock in sread:
                if sock == self.srvsock:
                    self.accept_new_connection()
                else:
                    str_send = sock.recv(1024).decode('utf-8')
                    print('send data: ', str_send)
                    host, port = sock.getpeername()
                    if str_send == 'exit':
                        str_send = 'Client left %s:%s\r\n' % (host, port)
                        self.broadcast_str(str_send, sock)
                        sock.close()
                        self.descripors.remove(sock)
                        break
                    else:
                        newstr = '[%s:%s] %s' % (host, port, str_send)
                        self.broadcast_str(str_send, sock)
 
    def accept_new_connection(self):
        newsock, (remhost, remport) = self.srvsock.accept()
        self.descripors.append(newsock)
        print("clinet connected!", remhost)
        # newsock.send("You are Connected\n".encode('utf8'))
        # str_send = 'Client joined %s:%s' % (remhost, remport)
        # self.broadcast_str(str_send, newsock)
 
    def broadcast_str(self, str_send, my_sock):
        for sock in self.descripors:
            if sock != self.srvsock and sock != my_sock:
                sock.send(str_send.encode('utf-8'))
        # print("forward:"+str_send+"       --- successfully")
 

if __name__ == "__main__":
    TcpServer(6009).run()
