from logging import DEBUG, Formatter, getLogger
from logging.handlers import RotatingFileHandler
from socketserver import BaseRequestHandler, TCPServer

from cbus import Frame
from mpi_cs4 import CommandStation

CBUS_HOST, CBUS_PORT = "localhost", 5550

command_station = CommandStation()


class CBusTCPHandler(BaseRequestHandler):
    def setup(self):
        self._logger = getLogger("mpi_cs4")
        self._logger.info("%s:%d connected", *self.client_address)
        command_station.cb_send = self.send

    def finish(self):
        self._logger.info("%s:%d disconnected", *self.client_address)

    def send(self, frame: Frame):
        self._logger.info("OUT: %s", str(frame))
        self.request.sendall(frame.net_encoded_frame)

    def handle(self):
        while True:
            self.data = self.request.recv(32).strip()
            if len(self.data) == 0:
                # disconnect
                break
            else:
                req_frames = Frame.parse_from_network(self.data)
                for req_frame in req_frames:
                    self._logger.info("IN : %s", str(req_frame))
                    reply_frames = command_station(req_frame)
                    for frame in reply_frames:
                        self._logger.info("OUT: %s", str(frame))
                        self.request.sendall(frame.net_encoded_frame)


if __name__ == "__main__":
    handler = RotatingFileHandler("mpi_cs4.log", maxBytes=102400, backupCount=5)
    logger = getLogger("mpi_cs4")
    logger.setLevel(DEBUG)
    bf = Formatter("[{asctime} {name}] {levelname:8s} {message}", datefmt="%Y-%m-%d %H:%M:%S", style="{")
    handler.setFormatter(bf)
    logger.addHandler(handler)
    logger.info("MPI_CS4 start")
    with TCPServer((CBUS_HOST, CBUS_PORT), CBusTCPHandler) as server:
        server.serve_forever(poll_interval=10)
    logger.info("MPI_CS4 end")
