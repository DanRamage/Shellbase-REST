from flask import Flask, g, current_app
import logging.config
from logging.handlers import RotatingFileHandler
from logging import Formatter
import atexit


from flask_cors import CORS
from .shellbase_db import shellbase_db
from config import SECRET_API_KEY, SHELLBASE_CONNECTION_STRING, FULL_LOG_PATH
import signal

#from apispec import APISpec

db_conn = shellbase_db()

class GracefulKiller:
  kill_now = False
  def __init__(self, db_conn):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)
    self._db_conn = db_conn
  def exit_gracefully(self,signum, frame):
    current_app.logger.debug("exit_gracefully called, disconnecting database.")
    self._db_conn.disconnect()
    self.kill_now = True

def init_logging(app):
  app.logger.setLevel(logging.DEBUG)
  file_handler = RotatingFileHandler(filename = FULL_LOG_PATH)
  file_handler.setLevel(logging.DEBUG)
  file_handler.setFormatter(Formatter('%(asctime)s,%(levelname)s,%(module)s,%(funcName)s,%(lineno)d,%(message)s'))
  app.logger.addHandler(file_handler)

  app.logger.debug("Logging initialized")

  return

def build_url_rules(app):
    from .rest_views import ShellbaseStationsInfo, \
            ShellbaseAreas, \
            ShellbaseStateStationDataQuery, \
            ShellbaseSpatialDataQuery, \
            ShellbaseStationInfo, \
            APIHelp

    app.logger.debug("build_url_rules started")

    app.add_url_rule('/api/v1/help', view_func=APIHelp.as_view('api_help'))
    '''
    app.add_url_rule('/api/v1/<string:state>/areas',
                     view_func=ShellbaseAreas.as_view('areas_info_api'), methods=['GET'])
    '''
    app.add_url_rule('/api/v1/metadata/stations',
                     view_func=ShellbaseStationsInfo.as_view('station_info_api'), methods=['GET'])
    app.add_url_rule('/api/v1/metadata/stations/<string:state>',
                     view_func=ShellbaseStationsInfo.as_view('state_stations_info_api'), methods=['GET'])
    app.add_url_rule('/api/v1/metadata/stations/<string:state>/<string:station>',
                     view_func=ShellbaseStationInfo.as_view('state_station_info_api'), methods=['GET'])
    '''
    app.add_url_rule('/api/v1/data/',
                     view_func=ShellbaseSpatialDataQuery.as_view('spatial_station_data_api'), methods=['GET'])
    '''
    app.add_url_rule('/api/v1/data/<string:state>/<string:station>',
                     view_func=ShellbaseStateStationDataQuery.as_view('state_station_data_api'), methods=['GET'])


    @app.errorhandler(500)
    def internal_error(exception):
        app.logger.exception(exception)

    @app.errorhandler(404)
    def internal_error(exception):
        app.logger.exception(exception)

    @app.teardown_appcontext
    def remove_session(error):
        """Closes the database again at the end of the request."""
        if hasattr(g, 'db_session'):
            db_conn.remove_session()
            current_app.logger.debug("Removing DB Session.")

    @app.route('/resttest/hello')
    def hello_world():
        return 'Hello World!'

    app.logger.debug("build_url_rules finished")


def shutdown_all():
    current_app.logger.debug("shutdown_all called, disconnecting database.")
    db_conn.disconnect()

def create_app():
    #flask_app = Flask(__name__, static_url_path="", static_folder="static")
    flask_app = Flask(__name__)
    '''
    #Enable Cross origin
    if not PRODUCTION_MACHINE:
        cors = CORS(flask_app, resources={r"/cdmorestdata/login": {"origins": "*"},
                                          r"/cdmorestdata/updatealerts": {"origins": "*"},
                                          r"/cdmorestdata/togglealerts": {"origins": "*"},
                                          r"/cdmorestdata/deletealerts": {"origins": "*"}})
    else:
        cors = CORS(flask_app, resources={r"/resttest/cdmorestdata/login": {"origins": "*"},
                                          r"/resttest/cdmorestdata/updatealerts": {"origins": "*"},
                                          r"/resttest/cdmorestdata/togglealerts": {"origins": "*"},
                                          r"/resttest/cdmorestdata/deletealerts": {"origins": "*"}})
    '''
    flask_app.secret_key = SECRET_API_KEY
    init_logging(flask_app)

    db_conn.connectDB(SHELLBASE_CONNECTION_STRING)

    build_url_rules(flask_app)

    atexit.register(shutdown_all)
    try:
        killer = GracefulKiller(db_conn)
    except Exception as e:
        flask_app.logger.exception(e)
    return flask_app

#app = create_app()

def get_db_conn():
    """Opens a new database connection if there is none yet for the
    current application context.
    """
    if not hasattr(g, 'db_session'):
        g.rts_session = True
    current_app.logger.debug("Returning DB Session.")
    return db_conn.Session()

if __name__ == '__main__':
    app.run()
    init_logging(app)
    app.logger.debug("Run started.")
