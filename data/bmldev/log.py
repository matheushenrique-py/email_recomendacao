from datetime import datetime

class Log():

    def __init__(self, path):
        self.path = path
        with open(self.path, 'w') as log: 
            log.write('{} -> [Starting]\n'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    def reg(self, msg):
        with open(self.path, 'a') as log: 
            log.write('{} -> [{}]\n'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S'), msg))