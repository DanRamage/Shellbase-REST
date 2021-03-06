from flask import request, render_template, current_app, session, Response, jsonify
from flask.views import View, MethodView
import pandas as pd
import geopandas as gpd
from sqlalchemy import and_, func
from shapely import wkt
import json
from datetime import datetime
import time
from itertools import chain

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
JSON_RETURN = 1
CSV_RETURN = 2
class ShellbaseAPIBase(MethodView):
    def __init__(self):
        self._return_type = JSON_RETURN

    def get_request_args(self):
        if 'type' in request.args:
            if request.args['type'] == 'csv':
                self._return_type = CSV_RETURN
        return None
    def get_response(self, **kwargs):
        if self._return_type == JSON_RETURN:
            return self.geojson_response(**kwargs)
        else:
            return self.csv_response(**kwargs)
    def geojson_response(self, **kwargs):
        return Response(json.dumps({}), 404, content_type='Application/JSON')
    def csv_response(self, **kwargs):
        return Response('', 404, content_type='text/csv')

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
            with get_db_conn() as db_obj:
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

class ShellbaseStationsInfo(ShellbaseAPIBase):
    def __init__(self):
        super().__init__()
        self._bbox = None
        self._return_type = JSON_RETURN

    def get(self, state=None):
        from shellbaseapi import get_db_conn

        req_start_time = time.time()
        self.get_request_args()
        current_app.logger.debug("IP: %s start query stations, State: %s BBOX: %s metadata"\
                                 % (request.remote_addr, state, self._bbox))
        try:
            with get_db_conn() as db_obj:
                if 'bbox' in request.args:
                    resp = self.spatial_query_features(state, db_obj)
                else:
                    resp = self.query_features(state, db_obj)
        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')

        current_app.logger.debug("IP: %s finished query stations, State: %s BBOX: %s metadata in %f seconds"\
                                 % (request.remote_addr, state, self._bbox, time.time()-req_start_time))

        return resp
    def get_request_args(self):
        super().get_request_args()
        if 'bbox' in request.args:
            self._bbox = self.BBOXtoPolygon(request.args['bbox'])

    def query_features(self, state, db_obj):
        try:
            from .shellbase_models import Stations, Areas, Lkp_Area_Classification
            #The isouter=True gives us a left join.
            recs_q = db_obj.query(Stations, Areas.name, Lkp_Area_Classification.name)\
                .join(Areas, Areas.id == Stations.area_id, isouter=True)\
                .join(Lkp_Area_Classification, Lkp_Area_Classification.id == Areas.classification, isouter=True)\
                .order_by(Stations.state)
            if state:
                recs_q = recs_q.filter(Stations.state == state.upper())

            recs = recs_q.all()
            resp = self.get_response(recs=recs, db_obj=db_obj, state=state)

        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')
        return resp
    def spatial_query_features(self, state, db_obj):
        from .shellbase_models import Stations, Areas, Lkp_Area_Classification

        features = {
            'type': 'FeatureCollection',
            'features': []
        }

        try:
            if self._bbox:
                bbox_series = gpd.GeoSeries([self._bbox])
                bbox_df = gpd.GeoDataFrame({'geometry': bbox_series})
            #The isouter=True gives us a left join.
            #We provide lables for the Area.name and Lkp_Area_Classification.name columns
            #to clearly know which column we are working with after the GeoPandas operation.
            #Otherwise sqlalchemy uses it's own naming convention like name_1, name_2.
            recs_q = db_obj.query(Stations, Areas.name.label('area_name'), Lkp_Area_Classification.name.label('classification_name'))\
                .join(Areas, Areas.id == Stations.area_id, isouter=True)\
                .join(Lkp_Area_Classification, Lkp_Area_Classification.id == Areas.classification, isouter=True) \
                .order_by(Stations.state)

            if state:
                recs_q = recs_q.filter(Stations.state == state.upper())
            if self._bbox:
                #We give pandas the sql statement to make the query and build the dataframe from the results.
                df = pd.read_sql(recs_q.statement, db_obj.bind)
                #Create the geopandas dataframe telling using the points_from_xy to build the geometry column.
                geo_df = gpd.GeoDataFrame(df,
                                          geometry=gpd.points_from_xy(x=df.long, y=df.lat))
                #Taking the passed in bounding box, we do an intersection to get the stations we are interested in.
                overlayed_stations = gpd.overlay(geo_df, bbox_df, how="intersection", keep_geom_type=False)

                resp = self.get_response(state=state, recs=overlayed_stations, db_obj=db_obj)

        except Exception as e:
            current_app.logger.exception(e)
            resp = Response(json.dumps({'message': "Server error processing request."}), 404, content_type='Application/JSON')

        return resp

    def csv_response(self, **kwargs):
        features = []
        recs = kwargs.get('recs', [])
        db_obj = kwargs['db_obj']
        state = kwargs['state']
        current_state = None
        if type(recs) == gpd.GeoDataFrame:
                #for index, row in recs.iterrows():
                for index, row in recs.iterrows():
                    classification = ''
                    if row.classification_name is not None:
                        classification = row['classification_name']
                    row = [
                                row['name'],
                                float(row.geometry.x),
                                float(row.geometry.y),
                                row['state'],
                                 row['active'],
                                 row['area_name'],
                                 classification
                                 ]
                    features.append(row)

        else:
            for index, rec in enumerate(recs):
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
                classification = ''
                if rec[2] is not None:
                    classification = rec[2]

                row = [
                    rec.Stations.name,
                    long,
                    lat,
                    rec.Stations.state,
                    rec.Stations.active,
                    rec[1],
                    classification
                ]
                features.append(row)

        header = ['name', 'longitude', 'latitude', 'state', 'active', 'area',
                  'classification']

        out_string = []
        out_string.append(",".join(header))
        for row in features:
            out_string.append(",".join(map(str,row)))
        out_string = "\n".join(out_string)

        data_type = "ALL"
        if state:
            data_type = state=state.upper()
        filename = "{type}_Stations_Metadata".format(type=data_type)

        resp = Response(out_string, 200, content_type="text/csv",
                        headers={"content-disposition": "attachment;filename=" + filename}
                        )
        return resp

    def geojson_response(self, **kwargs):
        features = {
            'type': 'FeatureCollection',
            'features': []
        }
        recs = kwargs.get('recs', [])
        db_obj = kwargs['db_obj']
        if type(recs) == gpd.GeoDataFrame:
                sample_types = []
                #for index, row in recs.iterrows():
                for index, row in recs.iterrows():

                    properties = {}
                    properties['name'] = row['name']
                    properties['state'] = row['state']
                    properties['active'] = row['active']
                    properties['area'] = row['area_name']
                    properties['classification'] = ''
                    if row.classification_name is not None:
                        properties['classification'] = row['classification_name']
                    properties['sample_types'] = sample_types

                    features['features'].append({
                        'type': 'Feature',
                        "geometry": {
                            "type": "Point",
                            "coordinates": [float(row.geometry.x), float(row.geometry.y)]
                        },
                        'properties': properties
                    })
        else:
            sample_types = []
            current_state = None
            for index, rec in enumerate(recs):
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
                properties = {}
                properties['name'] = rec.Stations.name
                properties['state'] = rec.Stations.state
                properties['active'] = rec.Stations.active
                properties['area'] = rec[1]

                properties['classification'] = ''
                if rec[2] is not None:
                    properties['classification'] = rec[2]

                features['features'].append({
                    'type': 'Feature',
                    "geometry": {
                        "type": "Point",
                        "coordinates": [long, lat]
                    },
                    'properties': properties
                })
        resp = jsonify(features)
        return resp

class ShellbaseStationInfo(ShellbaseAPIBase):
    def get(self, state=None, station=None):
        from shellbaseapi import get_db_conn

        req_start_time = time.time()
        self.get_request_args()
        current_app.logger.debug("IP: %s start query station, State: %s Station: %s metadata"\
                                 % (request.remote_addr, state, station))
        try:
            with get_db_conn() as db_obj:
                resp = self.query_features(state, station, db_obj)
        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')

        current_app.logger.debug("IP: %s finished query station, State: %s Station: %s metadata in %f seconds"\
                                 % (request.remote_addr, state, station, time.time()-req_start_time))

        return resp
    def get_request_args(self):
        super().get_request_args()
        if 'bbox' in request.args:
            self._bbox = self.BBOXtoPolygon(request.args['bbox'])

    def query_features(self, state, station, db_obj):
        from .shellbase_models import Stations, Areas, Lkp_Area_Classification, Samples, Lkp_Sample_Type, Lkp_Sample_Units
        try:
            #The isouter=True gives us a left join.
            recs_q = db_obj.query(Stations, Areas.name, Lkp_Area_Classification.name)\
                .filter(Stations.name == station)\
                .filter(Stations.state == state.upper())\
                .join(Areas, Areas.id == Stations.area_id, isouter=True)\
                .join(Lkp_Area_Classification, Lkp_Area_Classification.id == Areas.classification, isouter=True)

            recs = recs_q.all()
            resp = self.get_response(state=state, recs=recs, db_obj=db_obj)
        except Exception as e:
            current_app.logger.exception(e)
            resp = Response({}, 404, content_type='Application/JSON')
        return resp

    def get_station_observation_information(self, state, station_name, db_obj):
        from .shellbase_models import Stations, Samples, Lkp_Sample_Type, Lkp_Sample_Units
        try:
            observation_info = []
            recs_q = db_obj.query(Stations, Samples, Lkp_Sample_Type, Lkp_Sample_Units) \
                .filter(Stations.name == station_name)\
                .filter(Stations.state == state)\
                .join(Samples, Samples.station_id == Stations.id)\
                .join(Lkp_Sample_Type, Lkp_Sample_Type.id == Samples.type_id)\
                .join(Lkp_Sample_Units, Lkp_Sample_Units.id == Samples.units_id)\
                .distinct(Samples.type_id)
            recs_q.all()

            for index,rec in enumerate(recs_q):
                if self._return_type == JSON_RETURN:
                    observation_info.append( {
                        'sample type': rec.Lkp_Sample_Type.name,
                        'sample units': rec.Lkp_Sample_Units.name
                    })
                else:
                    val = "{type} - {units}".format(type=rec.Lkp_Sample_Type.name,
                                                    units=rec.Lkp_Sample_Units.name)
                    observation_info.append(val)
        except Exception as e:
            current_app.logger.exception(e)
        return observation_info

    def get_station_timeframe(self, state, station_name, db_obj):
        start_date = end_date = None
        from .shellbase_models import Stations, Samples
        try:
            recs_q = db_obj.query(func.min(Samples.sample_datetime),func.max(Samples.sample_datetime)).select_from(Stations) \
                .filter(Stations.name == station_name)\
                .filter(Stations.state == state)\
                .join(Samples, Samples.station_id == Stations.id, isouter=True)
            recs_q.all()
            for rec in recs_q:
                start_date = rec[0]
                end_date = rec[1]
        except Exception as e:
            current_app.logger.exception(e)
        return(start_date,end_date)
    def get_request_args(self):
        super().get_request_args()
        if 'bbox' in request.args:
            self._bbox = self.BBOXtoPolygon(request.args['bbox'])

    def csv_response(self, **kwargs):
        features = []
        recs = kwargs.get('recs', [])
        db_obj = kwargs['db_obj']
        for index, rec in enumerate(recs):
            start_date, end_date = self.get_station_timeframe(rec.Stations.state, rec.Stations.name, db_obj)
            # Get the observations that a station has.
            sample_types = self.get_station_observation_information(rec.Stations.state, rec.Stations.name, db_obj)
            sample_types_col = ", ".join(f'{obs}'.format(obs) for obs in sample_types)
            sample_types_col = '\"%s\"' % (sample_types_col)
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

            classification = ''
            if rec[2] is not None:
                classification = rec[2]

            row = [
                long,
                lat,
                rec.Stations.name,
                rec.Stations.state,
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d"),
                rec.Stations.active,
                rec[1],
                classification,
            ]
            row.append(sample_types_col)
            features.append(row)

        header = ['longitude', 'latitude', 'name', 'state', 'start date', 'end_date', 'active', 'area',
                  'classification', 'sample_types']

        out_string = []
        out_string.append(",".join(header))
        for row in features:
            out_string.append(",".join(map(str,row)))
        out_string = "\n".join(out_string)

        filename = "{state}_{station}_Stations_Metadata".format(state=rec.Stations.state, station=rec.Stations.name)

        resp = Response(out_string, 200, content_type="text/csv",
                        headers={"content-disposition": "attachment;filename=" + filename}
                        )
        return resp

    def geojson_response(self, **kwargs):
        feature = {
            'type': 'Feature',
            'feature': None,
            'geometry': None
        }
        recs = kwargs.get('recs', [])
        db_obj = kwargs['db_obj']
        for index, rec in enumerate(recs):
            start_date, end_date = self.get_station_timeframe(rec.Stations.state, rec.Stations.name, db_obj)
            # Get the observations that a station has.
            sample_types = self.get_station_observation_information(rec.Stations.state, rec.Stations.name, db_obj)

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

            properties = {}
            properties['name'] = rec.Stations.name
            properties['state'] = rec.Stations.state
            if start_date:
                properties['start_date'] = start_date.strftime("%Y-%m-%d")
            if end_date:
                properties['end_date'] = end_date.strftime("%Y-%m-%d")
            properties['active'] = rec.Stations.active
            properties['area'] = rec[1]
            properties['classification'] = ''
            if rec[2] is not None:
                properties['classification'] = rec[2]

            properties['sample types'] = sample_types
            feature = {
                'type': 'Feature',
                "geometry": {
                    "type": "Point",
                    "coordinates": [long, lat]
                },
                'properties': properties
            }
        resp = jsonify(feature)
        return resp

class ShellbaseStateStationDataQuery(ShellbaseAPIBase):
    def __init__(self):
        super().__init__()

        self._start_date = None
        self._end_date = None
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
                with get_db_conn() as db_obj:
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
                    resp = self.get_response(recs=recs, db_obj=db_obj, station=station,
                                             start_date = self._start_date, end_date=self._end_date)
            except Exception as e:
                current_app.logger.exception(e)
                resp = Response(json.dumps({}), 500, content_type='Application/JSON')

        current_app.logger.debug("IP: %s finished ShellbaseStateStationDataQuery, State: %s Station: %s in %f seconds"\
                                 % (request.remote_addr,
                                    state,
                                    station,
                                    time.time()-req_start_time))
        return resp

    def get_request_args(self):
        super().get_request_args()
        if 'start_date' in request.args:
            self._start_date = request.args['start_date']
        else:
            raise APIError("start_date required parameter", 400)

        if 'end_date' in request.args:
            self._end_date = request.args['end_date']
        else:
            raise APIError("end_date required parameter", 400)
    def csv_response(self, **kwargs):
        from .shellbase_models import Stations, Samples, Lkp_Sample_Type, Lkp_Sample_Units
        recs =kwargs.get('recs', [])
        db_obj = kwargs['db_obj']
        station = kwargs['station']
        start_date = kwargs['start_date']
        end_date = kwargs['end_date']

        rows = []
        out_string = []

        try:
            sample_types = {}
            sample_type_units = {}
            column_indexes = {}
            header_row = ['Station', 'Datetime', 'Latitude', 'Longitude', 'Tide', "Sample Depth Type", "Sample Depth"]
            lat = -1.0
            long = -1.0
            current_row_datetime = None
            row = []
            for index, rec in enumerate(recs):
                if index == 0:
                    # Get the observations the station should have.
                    recs_q = db_obj.query(Stations, Samples, Lkp_Sample_Type, Lkp_Sample_Units) \
                        .filter(Stations.name == station) \
                        .join(Samples, Samples.station_id == Stations.id) \
                        .join(Lkp_Sample_Type, Lkp_Sample_Type.id == Samples.type_id) \
                        .join(Lkp_Sample_Units, Lkp_Sample_Units.id == Samples.units_id) \
                        .distinct(Samples.type_id)
                    recs_q.all()
                    for samples_rec in recs_q:
                        sample_types[samples_rec.Lkp_Sample_Type.id] = samples_rec.Lkp_Sample_Type.name
                        sample_type_units[samples_rec.Lkp_Sample_Type.id] = samples_rec.Lkp_Sample_Units.name
                        header_row.append('{name}-{units}'.format(name=samples_rec.Lkp_Sample_Type.name,
                                                                  units=samples_rec.Lkp_Sample_Units.name))
                        column_indexes[samples_rec.Lkp_Sample_Type.id] = len(header_row) -1
                rec_datetime = rec.Samples.sample_datetime.strftime("%Y-%m-%d %H:%M:%S")
                #When we get a new date and time, we need a new row.
                if current_row_datetime != rec.Samples.sample_datetime:
                    try:
                        lat = float(rec.Stations.lat)
                    except TypeError as e:
                        e
                    try:
                        long = float(rec.Stations.long)
                    except TypeError as e:
                        e
                    #Create all the columns we will have in the row.
                    row = [''] * len(header_row)
                    rows.append(row)
                    #Here we set the bits that are common for the row.
                    row[0] = station
                    row[1] = rec_datetime
                    row[2] = lat
                    row[3] = long
                    row[4] = rec.Lkp_Tide.name
                    row[5] = rec.Samples.sample_depth_type
                    row[6] = rec.Samples.sample_depth
                    current_row_datetime = rec.Samples.sample_datetime
                col_ndx = column_indexes[rec.Samples.type_id]
                row[col_ndx] = rec.Samples.value

            out_string.append(",".join(header_row))
            for row in rows:
                out_string.append(",".join(map(str, row)))
            out_string = "\n".join(out_string)
            filename = "{station}_{start_date}_to_{end_date}".format(station=station,
                                                                     start_date=start_date,
                                                                     end_date=end_date)
            resp = Response(out_string, 200, content_type="text/csv",
                            headers={"content-disposition":"attachment;filename=" + filename}
            )

        except Exception as e:
            current_app.logger.exception(e)
            resp = Response(json.dumps({'message': "Server error processing request"}, 404))

        return resp

    def geojson_response(self, **kwargs):
        features = {
            'type': 'Feature',
            'geometry': {},
            'properties': {}
        }
        recs =kwargs.get('recs', [])
        # For the observations and tide, we add a key that is the observation name. Then
        # we add a list of the datetime and value fields. For example the
        # FC data would have an entry like:
        # fc:
        #   datetime: [2020-01-01]
        #   value: [10]
        # The value and datetime are indexed together.
        properties = features['properties']
        try:
            for index, rec in enumerate(recs):
                rec_datetime = rec.Samples.sample_datetime.strftime("%Y-%m-%d %H:%M:%S")
                # Tide is not a separate observation, it tags along on each database record.
                if 'tide' not in properties:
                    properties['tide'] = {'value': [], 'datetime': []}
                if rec_datetime not in properties['tide']['datetime']:
                    properties['tide']['value'].append(rec.Lkp_Tide.name)
                    properties['tide']['datetime'].append(rec_datetime)
                obs_key = rec.Lkp_Sample_Type.name.replace(' ', '_')

                properties['sample_depth_type'] = rec.Samples.sample_depth_type
                properties['sample_depth'] = rec.Samples.sample_depth

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
                resp = jsonify(features)
        except Exception as e:
            current_app.logger.exception(e)
            resp = Response(json.dumps({'message': "Server error processing request"}, 404))
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