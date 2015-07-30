#import tcpserver
import AsyncTcpServer
if __name__ == '__main__':
    # Server = AsyncTcpServer.server()
    # Server.run()
    Server = AsyncTcpServer.TornadoServer()
    Server.run()