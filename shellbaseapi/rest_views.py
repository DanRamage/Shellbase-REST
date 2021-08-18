from flask import request, render_template, current_app, session, Response, jsonify
from flask.views import View, MethodView
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, Point
from shapely import wkt
import json
from datetime import datetime
import time


session_data = {}

class APIHelp(View):
    def dispatch_request(self):
        current_app.logger.debug('IP: %s APIHelp rendered' % (request.remote_addr))
        return render_template("api_help.html")

class ResponseError(Exception):
    def __init__(self, arg1, status):
        self._status = status
        self._source = None
        self._title = None
        self._detail = None
        self._message = arg1
        super().__init__(arg1)

    def as_dict(self):
        return {
            "status": self.status,
            "source": {"pointer": self.source},
            "title": self.title,
            "detail": self.detail
        }
    def get_response(self):
        return Response({})

class APIError(Exception):
    def __init__(self, arg1, status):
        super().__init__(arg1)
        self._message = arg1
        self._status = status
    def get_response(self):

        msg = "An error occured with the query."
        query_error = self._message

        return Response(json.dumps({'message': msg, 'error': query_error}),
                status=self._status,
                mimetype='Application/JSON')

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

class ShellbaseAPIBase(MethodView):
    def get_request_args(self):
        return None
    def GeoJSONResponse(self, **kwargs):
        return Response(json.dumps({}), 404, content_type='Application/JSON')
    def CSVResponse(self, **kwargs):
        return Response(json.dumps({}), 404, content_type='text/csv')

    def BBOXtoPolygon(self, bbox):
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
        from shellbaseapi import get_db_conn
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
        from shellbaseapi import get_db_conn
        from .shellbase_models import Stations
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
        from shellbaseapi import get_db_conn
        from .shellbase_models import Stations

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


class ShellbaseStateStationDataQuery(ShellbaseAPIBase):
    def __init__(self):
        self._start_date = None
        self._end_date = None

    def get_request_args(self):
        if 'start_date' in request.args:
            self._start_date = request.args['start_date']
        else:
            raise APIError("start_date required parameter", 400)

        if 'end_date' in request.args:
            self._end_date = request.args['end_date']
        else:
            raise APIError("end_date required parameter", 400)


    def get(self, state, station):
        req_start_time = time.time()
        from shellbaseapi import get_db_conn
        from .shellbase_models import Samples, Stations, Lkp_Sample_Type, Lkp_Sample_Units, Lkp_Tide
        try:
            self.get_request_args()
            current_app.logger.debug("IP: %s start ShellbaseStateStationDataQuery, State: %s Station: %s Start: %s End: %s" \
                                     % (request.remote_addr,
                                        state,
                                        station,
                                        self._start_date,
                                        self._end_date))
        except APIError as e:
            resp = e.get_response()
        else:
            try:
                features = {
                    'type': 'Feature',
                    'geometry': {},
                    'properties': {}
                }                #Do we have a start/end date range to use? If not we're simply going to return the last sample
                #data point.
                db_obj = get_db_conn()
                recs_q = db_obj.query(Samples,Stations,Lkp_Sample_Type,Lkp_Sample_Units,Lkp_Tide)\
                    .join(Stations, Stations.id == Samples.station_id)\
                    .join(Lkp_Sample_Type, Lkp_Sample_Type.id == Samples.type_id)\
                    .join(Lkp_Sample_Units, Lkp_Sample_Units.id == Samples.units_id)\
                    .join(Lkp_Tide, Lkp_Tide.id == Samples.tide_id)
                if self._start_date:
                    recs_q = recs_q.filter(Samples.sample_datetime >= self._start_date)
                if self._end_date:
                    recs_q = recs_q.filter(Samples.sample_datetime < self._end_date)
                recs_q = recs_q.filter(Stations.name == station)\
                    .filter(Stations.state == state.upper())\
                    .order_by(Samples.sample_datetime)
                recs = recs_q.all()
                properties = features['properties']
                for index, rec in enumerate(recs):
                    rec_datetime = rec.Samples.sample_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    if 'tide' not in properties:
                        properties['tide'] = { 'value': [], 'datetime': [] }
                    if rec_datetime not in properties['tide']['datetime']:
                        properties['tide']['value'].append(rec.Lkp_Tide.name)
                        properties['tide']['datetime'].append(rec_datetime)
                    obs_key = rec.Lkp_Sample_Type.name.replace(' ', '_')
                    if obs_key not in properties:
                        properties[obs_key] = {'value': [], 'datetime': []}
                        properties[obs_key]['units'] = rec.Lkp_Sample_Units.name
                    properties[obs_key]['datetime'].append(rec.Samples.sample_datetime.strftime("%Y-%m-%d %H:%M:%S"))
                    properties[obs_key]['value'].append(rec.Samples.value)
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
                        features['geometry'] = {
                            "type": "Point",
                            "coordinates": [long, lat]
                        }

            except Exception as e:
                current_app.logger.exception(e)
                resp = Response(json.dumps({}), 500, content_type='Application/JSON')

            resp = jsonify(features)
        current_app.logger.debug("IP: %s finished ShellbaseStateStationDataQuery, State: %s Station: %s in %f seconds"\
                                 % (request.remote_addr,
                                    state,
                                    station,
                                    time.time()-req_start_time))
        return resp


class ShellbaseSpatialDataQuery(ShellbaseAPIBase):
    def __init__(self):
        self._bbox = None
        self._start_date = None
        self._end_date = None
    def get_request_args(self):
        if 'bbox' in request.args:
            self._bbox = self.BBOXtoPolygon(request.args['bbox'])
        else:
            raise APIError("BBOX required parameter", 400)
        if 'start_date' in request.args:
            self._start_date = request.args['start_date']
        else:
            raise APIError("start_date required parameter", 400)

        if 'end_date' in request.args:
            self._end_date = request.args['end_date']
        else:
            raise APIError("end_date required parameter", 400)

    def get(self):
        features = {
            'type': 'FeatureCollection',
            'features': []
        }
        try:
            from shellbaseapi import get_db_conn
            from .shellbase_models import Samples, Stations

            try:
                self.get_request_args()
            except APIError as e:
                resp = e.get_response()
            else:
                current_app.logger.debug("IP: %s start ShellbaseSpatialDataQuery, BBOX: %s" \
                                         % (request.remote_addr, self._bbox))
                # Do we have a start/end date range to use? If not we're simply going to return the last sample
                # data point.
                db_obj = get_db_conn()
                recs_q = db_obj.query(Samples, Stations) \
                    .join(Stations, Stations.id == Samples.station_id)
                if self._start_date:
                    recs_q = recs_q.filter(Samples.sample_datetime >= self._start_date)
                if self._end_date:
                    recs_q = recs_q.filter(Samples.sample_datetime < self._end_date)
                recs_q = recs_q.order_by(Samples.sample_datetime)

        except Exception as e:
            current_app.logger.exception(e)
            resp = (Response(json.dumps({'error': "Server unable to process request"}), status=500))
        return resp