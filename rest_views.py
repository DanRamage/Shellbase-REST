from flask import request, render_template, current_app, session, Response, jsonify
from flask.views import View, MethodView
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, Point
from shapely import wkt

from datetime import datetime
import time


session_data = {}

class APIHelp(View):
    def dispatch_request(self):
        current_app.logger.debug('IP: %s APIHelp rendered' % (request.remote_addr))
        return render_template("api_help.html")

class ResponseError(Exception):
    def __init__(self, arg1):
        self.status = None
        self.source = None
        self.title = None
        self.detail = None
        super(ResponseError, self).__init__(arg1)

    def as_dict(self):
        return {
            "status": self.status,
            "source": {"pointer": self.source},
            "title": self.title,
            "detail": self.detail
        }

class APIError(Exception):
    def __init__(self, arg1, status):
        super(ResponseError, self).__init__(arg1)
        self._status = status
    def as_dict(self):
        return {
            "status": self._status
        }

'''
Below are the API views.
'''

def BBOXtoPolygon(bbox):
    try:
        bounding_box = request.args['bbox'].split(',')
        if len(bounding_box) == 4:
            wkt_query_polygon = 'POLYGON(({x1} {y1}, {x1} {y2}, {x2} {y2}, {x2} {y1}, {x1} {y1}))'.format(
                x1=bounding_box[0],
                y1=bounding_box[1],
                x2=bounding_box[2],
                y2=bounding_box[3]
            )
            query_polygon = wkt.loads(wkt_query_polygon)
            return query_polygon
    except Exception as e:
        current_app.logger.exception(e)
    return None

class ShellbaseAreas(MethodView):
    def get(self, state=None):
        req_start_time = time.time()
        from app import get_db_conn
        from shellbase_models import Areas
        current_app.logger.debug("IP: %s start query areas, State: %s metadata" % (request.remote_addr, state))

        try:
            db_obj = get_db_conn()
            recs_q = db_obj.query(Areas).filter(Areas.state == state.upper())

            features = {
                'type': 'FeatureCollection',
                'features': []
            }
            recs = recs_q.all()
            for rec in recs:
                features['features'].append({
                    'type': 'Feature',
                    'properties': rec.as_dict()
                })
            resp = jsonify(features)
        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')
        resp.headers.add('Access-Control-Allow-Origin', '*')

        current_app.logger.debug("IP: %s finished query areas, State: %s metadata in %f seconds"\
                                 % (request.remote_addr,
                                    state,
                                    time.time()-req_start_time))

        return resp

class ShellbaseStationsInfo(MethodView):
    def get(self, state=None):
        req_start_time = time.time()
        bbox_arg = ""
        if 'bbox' in request.args:
            bbox_arg = request.args['bbox']
        current_app.logger.debug("IP: %s start query stations, State: %s BBOX: %s metadata"\
                                 % (request.remote_addr, state, bbox_arg))
        try:
            if 'bbox' in request.args:
                resp = self.spatial_query_features(state)
            else:
                resp = self.query_features(state)
        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')

        current_app.logger.debug("IP: %s finished query stations, State: %s BBOX: %s metadata in %f seconds"\
                                 % (request.remote_addr, state, bbox_arg, time.time()-req_start_time))

        return resp

    def query_features(self, state):
        from app import get_db_conn
        from shellbase_models import Stations
        try:
            features = {
                'type': 'FeatureCollection',
                'features': []
            }

            db_obj = get_db_conn()
            if state:
                recs_q = db_obj.query(Stations).filter(Stations.state == state.upper())
            else:
                recs_q = db_obj.query(Stations)

            recs = recs_q.all()
            for rec in recs:
                lat = -1.0
                long = -1.0
                try:
                    lat = float(rec.lat)
                except TypeError as e:
                    e
                try:
                    long = float(rec.long)
                except TypeError as e:
                    e
                features['features'].append({
                    'type': 'Feature',
                    "geometry": {
                        "type": "Point",
                        "coordinates": [long, lat]
                    },
                    'properties': rec.as_dict()
                })
            resp = jsonify(features)
        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')
        return resp
    def spatial_query_features(self, state):
        from app import get_db_conn
        from shellbase_models import Stations

        features = {
            'type': 'FeatureCollection',
            'features': []
        }

        try:
            query_polygon = None

            bounding_box = request.args['bbox'].split(',')
            if len(bounding_box) == 4:
                wkt_query_polygon = 'POLYGON(({x1} {y1}, {x1} {y2}, {x2} {y2}, {x2} {y1}, {x1} {y1}))'.format(
                    x1=bounding_box[0],
                    y1=bounding_box[1],
                    x2=bounding_box[2],
                    y2=bounding_box[3]
                )
                query_polygon = wkt.loads(wkt_query_polygon)
                bbox_series = gpd.GeoSeries([query_polygon])
                bbox_df = gpd.GeoDataFrame({'geometry': bbox_series})

            db_obj = get_db_conn()
            if state:
                recs_q = db_obj.query(Stations).filter(Stations.state == state.upper())
            else:
                recs_q = db_obj.query(Stations)
            if query_polygon:
                df = pd.read_sql(recs_q.statement, db_obj.bind)
                geo_df = gpd.GeoDataFrame(df,
                                          geometry=gpd.points_from_xy(x=df.long, y=df.lat))
                overlayed_stations = gpd.overlay(geo_df, bbox_df, how="intersection", keep_geom_type=False)
                for index, row in overlayed_stations.iterrows():
                    sample_depth = ""
                    if row.sample_depth_type is not None:
                        sample_depth = row.sample_depth_type
                    features['features'].append({
                        'type': 'Feature',
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(row.geometry.x), float(row.geometry.y)]
                        },
                        'properties': {
                            'name': row.name,
                            'state': row.state,
                            'sample_depth_type': row.sample_depth_type,
                            'sample_depth': sample_depth,
                            'active': row.active
                        }
                    })
            resp = jsonify(features)

        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')

        return resp


class ShellbaseStateStationDataQuery(MethodView):
    def get(self, state, station):
        req_start_time = time.time()
        features = {
            'type': 'Feature',
            'geometry': {},
            'properties': {}
        }
        from app import get_db_conn
        from shellbase_models import Samples, Stations
        current_app.logger.debug("IP: %s start ShellbaseStateStationDataQuery, State: %s Station: %s"\
                                 % (request.remote_addr,
                                    state,
                                    station))

        try:
            start_date = end_date = None
            if 'start_date' in request.args:
                start_date = request.args['start_date']
            if 'end_date' in request.args:
                end_date = request.args['end_date']
            #Do we have a start/end date range to use? If not we're simply going to return the last sample
            #data point.
            db_obj = get_db_conn()
            recs_q = db_obj.query(Samples,Stations)\
                .join(Stations, Stations.id == Samples.station_id)
            if start_date:
                recs_q = recs_q.filter(Samples.sample_datetime >= start_date)
            if end_date:
                recs_q = recs_q.filter(Samples.sample_datetime < end_date)
            recs_q = recs_q.filter(Stations.name == station)\
                .filter(Stations.state == state.upper())\
                .order_by(Samples.sample_datetime)
            recs = recs_q.all()
            properties = features['properties']
            properties['sample_datetime'] = []
            properties['sample_value'] = []
            for index, rec in enumerate(recs):
                properties['sample_datetime'].append(rec.Samples.sample_datetime.strftime("%Y-%m-%d %H:%M:%S"))
                properties['sample_value'].append(rec.Samples.value)
                if index == 0:
                    lat = -1.0
                    long = -1.0
                    try:
                        lat = float(rec.Stations.lat)
                    except TypeError as e:
                        e
                    try:
                        long = float(rec.Stations.long)
                    except TypeError as e:
                        e
                    features['geomtry'] = {
                        "type": "Point",
                        "coordinates": [long, lat]
                    }

        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')

        resp = jsonify(features)
        current_app.logger.debug("IP: %s finished ShellbaseStateStationDataQuery, State: %s Station: %s in %f seconds"\
                                 % (request.remote_addr,
                                    state,
                                    station,
                                    time.time()-req_start_time))
        return resp


class ShellbaseSpatialDataQuery(MethodView):
    def get(self):
        features = {
            'type': 'FeatureCollection',
            'features': []
        }
        try:
            from app import get_db_conn
            from shellbase_models import Samples, Stations
            current_app.logger.debug("IP: %s start ShellbaseSpatialDataQuery, BBOX: %s" \
                                     % (request.remote_addr, bbox))

            start_date = end_date = None
            if 'start_date' in request.args:
                start_date = request.args['start_date']
            if 'end_date' in request.args:
                end_date = request.args['end_date']
            # Do we have a start/end date range to use? If not we're simply going to return the last sample
            # data point.
            db_obj = get_db_conn()
            recs_q = db_obj.query(Samples, Stations) \
                .join(Stations, Stations.id == Samples.station_id)
            if start_date:
                recs_q = recs_q.filter(Samples.sample_datetime >= start_date)
            if end_date:
                recs_q = recs_q.filter(Samples.sample_datetime < end_date)
            recs_q = recs_q.order_by(Samples.sample_datetime)

        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')
