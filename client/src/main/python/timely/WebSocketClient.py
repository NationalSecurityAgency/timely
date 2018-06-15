from tornado import escape
from tornado import gen
from tornado import ioloop
from tornado import websocket

import json


class WebSocketClient():
    """Base for web socket clients.
    """

    DEFAULT_CONNECT_TIMEOUT = 60
    DEFAULT_REQUEST_TIMEOUT = 60
    APPLICATION_JSON = 'application/json'

    def __init__(self, hostport, path, params, connect_timeout=DEFAULT_CONNECT_TIMEOUT,
                 request_timeout=DEFAULT_REQUEST_TIMEOUT):

        self.connect_timeout = connect_timeout
        self.request_timeout = request_timeout
        self.url = 'wss://' + hostport
        if (path is not None and len(path) > 0):
            if (path.startswith('/')):
                self.uri = self.uri + path
            else:
                self.uri = self.uri + '/' + path
        if (any(params.values())):
            first = True
            for key, value in params.iteritems():
                if (first == True):
                    self.url = self.url + '?'
                    first = False
                else:
                    self.url = self.url + '&'
                self.url = self.url + key + '=' + value

    def connect(self, ioloop=ioloop.IOLoop.current(instance=False)):
        """Connect to the server.
        :param str url: server URL.
        """
        request = websocket.httpclient.HTTPRequest(self.url, connect_timeout=self.connect_timeout, request_timeout=self.request_timeout, validate_cert=False)
        websocket.websocket_connect(request, io_loop=ioloop, callback=self._connect_callback)

    def send(self, data):
        """Send message to the server
        :param str data: message.
        """
        if not self._ws_connection:
            raise RuntimeError('Web socket connection is closed.')

        self._ws_connection.write_message(escape.utf8(json.dumps(data)))

    def close(self):
        """Close connection.
        """
        if not self._ws_connection:
            raise RuntimeError('Web socket connection is already closed.')

        self._ws_connection.close()
        self._ws_connection = None

    def _connect_callback(self, future):
        if future.exception() is None:
            self._ws_connection = future.result()
            self._on_connection_success()
            self._read_messages()
        else:
            self._on_connection_error(future.exception())

    @gen.coroutine
    def _read_messages(self):
        while True and not self._ws_connection is None:
            msg = yield self._ws_connection.read_message()
            if msg is None:
                self._on_connection_close()
                break

            self._on_message(msg)

    def _on_message(self, msg):
        """This is called when new message is available from the server.
        :param str msg: server message.
        """

        pass

    def _on_connection_success(self):
        """This is called on successful connection ot the server.
        """

        pass

    def _on_connection_close(self):
        """This is called when server closed the connection.
        """
        pass

    def _on_connection_error(self, exception):
        """This is called in case if connection to the server could
        not established.
        """

        pass

