'use strict';

System.register(['lodash', 'app/core/core', 'plugins/timely-app/components/auths'], function (_export, _context) {
  "use strict";

  var _, coreModule, appEvents, AuthsCtrl, _createClass, AuthsPageCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_lodash) {
      _ = _lodash.default;
    }, function (_appCoreCore) {
      coreModule = _appCoreCore.coreModule;
      appEvents = _appCoreCore.appEvents;
    }, function (_pluginsTimelyAppComponentsAuths) {
      AuthsCtrl = _pluginsTimelyAppComponentsAuths.AuthsCtrl;
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

      _export('AuthsPageCtrl', AuthsPageCtrl = function () {
        function AuthsPageCtrl(backendSrv, datasourceSrv) {
          _classCallCheck(this, AuthsPageCtrl);

          this.datasourceSrv = datasourceSrv;
          this.datasources = this.getTimelyDatasources();
          coreModule.controller('AuthsCtrl', AuthsCtrl);
        }

        _createClass(AuthsPageCtrl, [{
          key: 'getTimelyDatasources',
          value: function getTimelyDatasources() {
            var allSources = this.datasourceSrv.getAll();

            var sources = _.map(allSources, function (value, key, col) {
              var ret = {};
              ret.meta = {};
              ret.meta.type = col[key].meta.type;
              ret.meta.id = col[key].meta.id;
              ret.name = key;
              ret.jsonData = col[key].jsonData;
              return ret;
            });
            return _.filter(sources, function (d) {
              return d.meta.type === 'datasource' && d.meta.id === 'timely-datasource';
            });
          }
        }]);

        return AuthsPageCtrl;
      }());

      _export('AuthsPageCtrl', AuthsPageCtrl);

      AuthsPageCtrl.templateUrl = 'components/auths.html';
    }
  };
});
//# sourceMappingURL=authspage.js.map
