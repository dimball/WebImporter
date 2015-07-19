#import tcpserver
import AsyncTcpServer
if __name__ == '__main__':
    #tcpserver.TCPServer()
    Server = AsyncTcpServer.server()
    Server.run()
