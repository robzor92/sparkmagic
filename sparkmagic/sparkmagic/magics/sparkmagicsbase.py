# -*- coding: UTF-8 -*-

"""Runs Scala, PySpark and SQL statement through Spark using a REST endpoint in remote cluster.
Provides the %spark magic."""

# Copyright (c) 2015  aggftw@gmail.com
# Distributed under the terms of the Modified BSD License.

from __future__ import print_function
from IPython.core.magic import Magics, magics_class
from hdijupyterutils.ipythondisplay import IpythonDisplay

import sparkmagic.utils.configuration as conf
from sparkmagic.utils.sparklogger import SparkLog
from sparkmagic.utils.sparkevents import SparkEvents
from sparkmagic.utils.utils import get_sessions_info_html
from sparkmagic.utils.constants import MAGICS_LOGGER_NAME
from sparkmagic.livyclientlib.sparkcontroller import SparkController
from sparkmagic.livyclientlib.sqlquery import SQLQuery
from sparkmagic.livyclientlib.command import Command
from sparkmagic.livyclientlib.sparkstorecommand import SparkStoreCommand

from tqdm import tqdm_notebook as tqdm
from tqdm import tnrange
from time import sleep, time

@magics_class
class SparkMagicBase(Magics):
    def __init__(self, shell, data=None, spark_events=None):
        # You must call the parent constructor
        super(SparkMagicBase, self).__init__(shell)

        self.logger = SparkLog(u"SparkMagics")
        self.ipython_display = IpythonDisplay()
        self.spark_controller = SparkController(self.ipython_display)
                
        self.logger.debug("Initialized spark magics.")

        if spark_events is None:
            spark_events = SparkEvents()
        spark_events.emit_library_loaded_event()

    def execute_final(self, cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce):
        (success, out) = self.spark_controller.run_command(Command(cell), session_name)
        if not success:
            self.ipython_display.send_error(out)
        else:
            self.ipython_display.write(out)
            if output_var is not None:
                spark_store_command = self._spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce)
                df = self.spark_controller.run_command(spark_store_command, session_name)
                self.shell.user_ns[output_var] = df

    def execute_spark(self, cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce):

        if "lagom as" in cell:
            self.ipython_display.send_error("You are not allowed to do the following: 'import maggy.experiment.lagom as ...'. Please, just use 'import maggy.experiment as experiment' (or something else)")
            raise
        else if "lagom" in cell:
            self.ipython_display.write("Found lagom in cell")
            # 1. Get app_id using spark_controller and session_name
            app_id = self.spark_controller.get_app_id(session_name)
            self.ipython_display.write("App id is: " + app_id)            
            client = Client(app_id, 5, self.ipython_display)            
            try: 
                client.start_heartbeat()
                self.ipython_display.write("Started heartbeating...")
                self.execute_final(cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce)
            except:
                raise
            finally:
                # 4. Kill thread before leaving current scope                           
                client.stop()
                client.close()
        else:
            self.execute_final(cell, output_var, samplemethod, maxrows, samplefraction, session_name, coerce)

            
    @staticmethod
    def _spark_store_command(output_var, samplemethod, maxrows, samplefraction, coerce):
        return SparkStoreCommand(output_var, samplemethod, maxrows, samplefraction, coerce=coerce)

    def execute_sqlquery(self, cell, samplemethod, maxrows, samplefraction,
                         session, output_var, quiet, coerce):
        sqlquery = self._sqlquery(cell, samplemethod, maxrows, samplefraction, coerce)
        df = self.spark_controller.run_sqlquery(sqlquery, session)
        if output_var is not None:
            self.shell.user_ns[output_var] = df
        if quiet:
            return None
        else:
            return df

    @staticmethod
    def _sqlquery(cell, samplemethod, maxrows, samplefraction, coerce):
        return SQLQuery(cell, samplemethod, maxrows, samplefraction, coerce=coerce)

    def _print_endpoint_info(self, info_sessions, current_session_id):
        if info_sessions:
            info_sessions = sorted(info_sessions, key=lambda s: s.id)
            html = get_sessions_info_html(info_sessions, current_session_id)
            self.ipython_display.html(html)
        else:
            self.ipython_display.html(u'No active sessions.')


class Client(MessageSocket):
    """Client to register and await log events

    Args:
        :server_addr: a tuple of (host, port) pointing to the Server.
    """
    def __init__(self, app_id, hb_interval, ipython_display):
        # socket for heartbeat thread
        self.hb_sock = None
        self.hb_sock = None
        self.server_addr = None
        self.done = False
        self.hb_interval = hb_interval
        self.ipython_display = ipython_display        
        self.ipython_display.writeln("Starting Maggy Client")
        self._app_id = app_id
        self._maggy_ip = None
        self._maggy_port = None
        self._maggy_secret = None
        self._num_trials = None
        self._trials_todate = None                
            
    def _request(self, req_sock, msg_data=None):
        """Helper function to wrap msg w/ msg_type."""
        msg = {}
        msg['type'] = "LOG"

        if msg_data or ((msg_data == True) or (msg_data == False)):
            msg['data'] = msg_data

        done = False
        tries = 0
        while not done and tries < MAX_RETRIES:
            try:
                MessageSocket.send(self, req_sock, msg)
                done = True
            except socket.error as e:
                tries += 1
                if tries >= MAX_RETRIES:
                    raise
                print("Socket error: {}".format(e))
                req_sock.close()
                req_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                req_sock.connect(self.server_addr)

        resp = MessageSocket.receive(self, req_sock)

        return resp

    def close(self):
        """Close the client's sockets."""
        self.hb_sock.close()

    def start_heartbeat(self):

        def _heartbeat(self):

            # 2. Using app_id, get Maggy Ip/port/secret from Hopsworks
            res = False
            while res is False:
                try:
                    self.ipython_display.writeln("Looking for the maggy server...")                    
                    res = self._get_maggy_driver()
                except:
                    time.sleep(self.hb_interval)                    
                    pass

            self.ipython_display.writeln("Found the maggy server...")                                    
            # 3. Start thread running polling logs in Maggy.
            self.server_addr = (self._maggy_ip, self._maggy_port)
            self.hb_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.hb_sock.connect(self.server_addr)
            self.ipython_display.writeln("Connected to the maggy server...")

            resp = self._request(self.hb_sock,'LOG')            
            self._handle_message(resp)

            # self._num_trials is now 'set', and self._trials_todate
            while not self.done:
                with tqdm(total=self._num_trials) as pbar:
                    
                    resp = self._request(self.hb_sock,'LOG')
                    self.ipython_display.writeln("Received a msg from  maggy server...")                            
                    _ = self._handle_message(resp, pbar)
                    # sleep one second
                    time.sleep(self.hb_interval)
                    

        t = threading.Thread(target=_heartbeat, args=(self))
        t.daemon = True
        t.start()

        print("Started log heartbeat")

    def stop(self):
        """Stop the Clients's heartbeat thread."""
        self.done = True

    def _handle_message(self, msg, pbar):
        """
        Handles a  message dictionary. Expects a 'type' and 'data' attribute in
        the message dictionary.

        Args:
            sock:
            msg:

        Returns:

        """
        msg_type = msg['type']
        if msg_type == 'LOG':
            data = msg['data']
            self.ipython_display.writeln(data)                
#            self.ipython_display.html(html)

# https://towardsdatascience.com/progress-bars-in-python-4b44e8a4c482
# update 'pbar'. pbar.update(1)
        return
                
    def _get_maggy_driver(self):
        self.ipython_display.writeln(u"Asking Hopsworks")        
        try:
            method = hopster.HTTP_CONFIG.HTTP_GET
            self.ipython_display.writeln(u"Got Method")
            resource_url = hopster.DELIMITERS.SLASH_DELIMITER + \
                           hopster.REST_CONFIG.HOPSWORKS_REST_RESOURCE + hopster.DELIMITERS.SLASH_DELIMITER + \
                           "maggy" + hopster.DELIMITERS.SLASH_DELIMITER + "getDriver" + \
                           hopster.DELIMITERS.SLASH_DELIMITER + self.get_app_id() 
            self.ipython_display.writeln(u"got url")
            self.ipython_display.writeln(resource_url)            
            endpoint = os.environ[hopster.ENV_VARIABLES.REST_ENDPOINT_END_VAR]            
            self.ipython_display.writeln(endpoint)            
            connection = _get_http_connection(https=True)
            self.ipython_display.writeln(u"got connection")
            
            response = _send_request(connection, method, resource_url)
            resp_body = response.read()
            resp = json.loads(resp_body)

            # Reset values to 'None' if empty string returned
            self._maggy_ip = resp[u"host_ip"]
            self._maggy_port = resp[u"port"]
            self._maggy_secret = resp[u"secret"]
        except:
            self.ipython_display.writeln("Hopsworks not home...")        

    def _get_hopsworks_rest_endpoint():
        elastic_endpoint = os.environ[hopster.ENV_VARIABLES.REST_ENDPOINT_END_VAR]
        return elastic_endpoint

            
    def _get_host_port_pair():
        endpoint = _get_hopsworks_rest_endpoint()
        if 'http' in endpoint:
            last_index = endpoint.rfind('/')
            endpoint = endpoint[last_index + 1:]
            host_port_pair = endpoint.split(':')
            return host_port_pair
        
    def _get_http_connection(https=False):
        host_port_pair = _get_host_port_pair()
        if (https):
            PROTOCOL = ssl.PROTOCOL_TLSv1_2
            ssl_context = ssl.SSLContext(PROTOCOL)
            connection = http.HTTPSConnection(str(host_port_pair[0]), int(host_port_pair[1]), context = ssl_context)
        else:
            connection = http.HTTPConnection(str(host_port_pair[0]), int(host_port_pair[1]))
            return connection

    def _get_jwt():
        with open(hopster.REST_CONFIG.JWT_TOKEN, "r") as jwt:
            return jwt.read()

    def _send_request(connection, method, resource, body=None):
        headers = {}
        headers[hopster.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + _get_jwt()
        connection.request(method, resource, body, headers)
        response = connection.getresponse()
        if response.status == hopster.HTTP_CONFIG.HTTP_UNAUTHORIZED:
            headers[hopster.HTTP_CONFIG.HTTP_AUTHORIZATION] = "Bearer " + _get_jwt()
            connection.request(method, resource, body, headers)
            response = connection.getresponse()
        return response
