'use strict';

System.register(['lodash', 'plugins/timely-app/components/authspage'], function (_export, _context) {
  "use strict";

  var _, AuthsPageCtrl, _createClass, MetricsPageCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_lodash) {
      _ = _lodash.default;
    }, function (_pluginsTimelyAppComponentsAuthspage) {
      AuthsPageCtrl = _pluginsTimelyAppComponentsAuthspage.AuthsPageCtrl;
    }],
    execute: function () {
      _createClass = function () {
        function defineProperties(target, props) {
          for (var i = 0; i < props.length; i++) {
            var descriptor = props[i];
            descriptor.enumerable = descriptor.enumerable || false;
            descriptor.configurable = true;
            if ("value" in descriptor) descriptor.writable = true;
            Object.defineProperty(target, descriptor.key, descriptor);
          }
        }

        return function (Constructor, protoProps, staticProps) {
          if (protoProps) defineProperties(Constructor.prototype, protoProps);
          if (staticProps) defineProperties(Constructor, staticProps);
          return Constructor;
        };
      }();

      _export('MetricsPageCtrl', MetricsPageCtrl = function () {
        function MetricsPageCtrl(backendSrv, datasourceSrv) {
          _classCallCheck(this, MetricsPageCtrl);

          this.hasDatasource = false;
          this.backendSrv = backendSrv;
          var apc = new AuthsPageCtrl(backendSrv, datasourceSrv);
          this.datasources = apc.getTimelyDatasources();
          if (this.datasources.length > 0) {
            this.source = this.datasources[0];
            this.updateDatasource();
          }
        }

        _createClass(MetricsPageCtrl, [{
          key: 'updateDatasource',
          value: function updateDatasource() {
            var _this = this;

            this._clearMetrics();
            var uri = this._getURI(this.source.jsonData);
            this.backendSrv.request({ method: 'GET',
              url: uri,
              withCredentials: true,
              params: "" }).then(function (response) {
              _this.metrics = _this._transformMetrics(response.metrics);
              _this.hasDatasource = true;
            });
          }
        }, {
          key: '_clearMetrics',
          value: function _clearMetrics() {
            this.metrics = [];
          }
        }, {
          key: '_transformMetrics',
          value: function _transformMetrics(metrics) {
            var ret = [];
            _.each(metrics, function (value, idx) {
              var metric = {};
              metric.metric = value.metric;
              metric.tags = [];
              _.map(value.tags, function (tag, key) {
                //        result || (result = {});
                //        result['name'] = tag.key;
                //        result['values'] || (result['values'] = []);
                //        result['values'].push(tag.value);
                var gIdx = _.findIndex(metric.tags, { 'name': tag.key });
                var grouped;
                if (gIdx == -1) {
                  grouped = { name: '', values: [] };
                } else {
                  grouped = metric.tags[gIdx];
                }
                grouped.values.push(tag.value);
                grouped.values = _.sortBy(_.uniq(grouped.values));
                grouped.name = tag.key;
                if (gIdx > -1) {
                  metric.tags[gIdx] = grouped;
                } else {
                  metric.tags.push(grouped);
                }
              }.bind(metric));
              metric.tags = _.sortBy(metric.tags, 'name');
              ret.push(metric);
            }.bind(ret));

            return _.sortBy(ret, 'metric');
          }
        }, {
          key: '_getURI',
          value: function _getURI(jsonData) {
            return 'https://' + jsonData.timelyHost + ':' + jsonData.httpsPort + '/api/metrics';
          }
        }]);

        return MetricsPageCtrl;
      }());

      _export('MetricsPageCtrl', MetricsPageCtrl);

      MetricsPageCtrl.templateUrl = 'components/metrics.html';
    }
  };
});
//# sourceMappingURL=metrics.js.map
