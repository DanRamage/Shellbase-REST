from flask import request, render_template, current_app, session, Response, jsonify
from flask.views import View, MethodView
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, Point
from shapely import wkt


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


class ShellbaseAreas(MethodView):
    def get(self, state=None):
        from app import get_db_conn
        from shellbase_models import Areas
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
        return resp

class ShellbaseStationsInfo(MethodView):
    def get(self, state=None):
        try:
            if 'bbox' in request.args:
                resp = self.spatial_query_features(state)
            else:
                resp = self.query_features(state)
        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')

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
                            'sample_depth': row.sample_depth,
                            'active': row.active
                        }
                    })
            resp = jsonify(features)

        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')

        return resp