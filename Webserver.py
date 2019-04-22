from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import requests

PORT_NUMBER = 8080
ENDPOINT = ''

class RequestHandler(BaseHTTPRequestHandler):

    valid_sources = ['signals', 'patterns']
    valid_signal_commands = ['norm', 'zscore', 'combine']

    def _set_headers(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()

    def _return_data(self, data):
        self._set_headers()
        self.wfile.write(json.dumps(data).encode('UTF-8'))

    def _get_data(self, source, id, sorted=False, sort_key=''):
        response = requests.get('{}/{}/{}'.format(ENDPOINT, source, id))
        if response.status_code == 404:
            self._send_error("Invalid id", '1-6', id)
            self._send_error("Invalid id", '1-6', id)
        if sorted:
            json_obj = json.loads(response.text)
            json_obj.sort(key=lambda k: k[sort_key])
            return json_obj
        return json.loads(response.text)

    def _send_error(self, error, expected, actual):
        payload = {}
        payload['error'] = '{}, expected {}, found {}'.format(error, expected, actual)
        self._return_data(payload)

    def _calc_zscore(self, source, id, parameters):
        data = self._get_data(source, id, sorted=True, sort_key='date')
        if data == {}:
            return
        if not parameters or 'window' not in parameters or '=' not in parameters:
            self._send_error('Invalid parameters', 'window=<window>', parameters)
        window_str = parameters.split('=')[1]
        try:
            int(window_str)
        except ValueError:
            self._send_error('Invalid window value', 'int', window_str)
            return

        window = int(window_str)
        if window > len(data) or window < 1:
            self._send_error('Invalid window value', '0 < window < {}'.format(len(data)), window)
            return

        sum = 0
        for i in range(window):
            sum += data[i]['value']
        mean = sum / window

        mean_diff_squared = 0
        for i in range(window):
            mean_diff_squared += (data[i]['value'] - mean) ** 2
        std_dev = (mean_diff_squared / window) ** .5

        for tuple in data:
            tuple['value'] = (tuple['value'] - mean) / std_dev

        self._return_data(data)

    def _normalize_data(self, source, id, normalizer):
        data = self._get_data(source, id, sorted=True, sort_key='date')
        if data == {}:
            return
        min = data[0]['value']
        max = data[0]['value']
        for tuple in data:
            min = tuple['value'] if tuple['value'] < min else min
            max = tuple['value'] if tuple['value'] > max else max
        for tuple in data:
            tuple['value'] = (tuple['value'] - min) * normalizer / (max - min)
        self._return_data(data)

    def _validate_linear_parameters(self, parameter):
        if 'signal' not in parameter or '=' not in parameter:
            self.send_error('Invalid parameter', 'signal=<id>,<weight>', parameter)
            return False, 0, 0
        sub_list = parameter.split('=')
        if sub_list[0] != 'signal':
            self.send_error('Invalid parameter', 'signal=<id>,<weight>', parameter)
            return False, 0, 0
        if ',' not in sub_list[1]:
            self.send_error('Invalid parameter', 'signal=<id>,<weight>', parameter)
            return False, 0, 0
        args_list = sub_list[1].split(',')
        if len(args_list) != 2:
            self.send_error('Invalid parameter', 'signal=<id>,<weight>', parameter)
            return False, 0, 0

        id = args_list[0]
        weight = args_list[1]

        try:
            id = int(id)
        except ValueError:
            self.send_error('Invalid id value', 'int', id)
            return False, 0, 0

        try:
            weight = float(weight)
        except ValueError:
            self.send_error('Invalid weight value', 'float', weight)
            return False, 0, 0

        return True, id, weight

    def _linear_combination(self, source, parameters):
        parameter_list = parameters.split('&')
        data_list = []
        weight_list = []
        payload = {}
        for parameter in parameter_list:
            success, id, weight = self._validate_linear_parameters(parameter)
            if not success:
                return
            data_blob = self._get_data(source, id, sorted=True, sort_key='date')
            if data_blob == {}:
                return
            data_list.append(data_blob)
            weight_list.append(weight)

        # Assuming that all the data series have the same length
        for i in range(len(data_list[0])):
            val = 0
            for j in range(len(parameter_list)):
                val += data_list[j][i]['value'] * weight_list[j]
            payload[data_list[j][i]['date']] = val

        json_obj = json.dumps({'results': payload})

        self._return_data(json_obj)

    def _process_signal(self, source, command, id, parameters):
        if command == 'norm':
            return self._normalize_data(source, id, 100)
        if command == 'zscore':
            return self._calc_zscore(source, id, parameters)
        if command == 'combine':
            return self._linear_combination(source, parameters)

    def _process_pattern(self):
        return ''

    def do_GET(self):
        path_list = self.path.split('/')[1:]
        if len(path_list) < 3:
            self._send_error('Insufficient number of parameters', '3', str(len(path_list)))
            return

        source = path_list[0]
        if source not in self.valid_sources:
            self._send_error('Invalid source', ''.join(str(e) + ' ' for e in self.valid_sources), source)
            return

        command = path_list[1]
        id = path_list[2]
        parameters = ''
        if '?' in path_list[2]:
            id = path_list[2].split('?')[0]
            parameters = path_list[2].split('?')[1]
        if source == 'signals':
            if command not in self.valid_signal_commands:
                self._send_error('Invalid signal command',
                                ''.join(str(e) + ' ' for e in self.valid_signal_commands), command)
                return
            self._process_signal(source, command, id, parameters)
        elif source == 'patterns':
            self._process_pattern()


try:
    server = HTTPServer(('', PORT_NUMBER), RequestHandler)
    print('Started httpserver on port', PORT_NUMBER)
    server.serve_forever()

except KeyboardInterrupt:
    server.socket.close()
