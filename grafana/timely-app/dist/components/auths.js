'use strict';

System.register(['lodash'], function (_export, _context) {
  "use strict";

  var _, _createClass, AuthsCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_lodash) {
      _ = _lodash.default;
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

      _export('AuthsCtrl', AuthsCtrl = function () {
        function AuthsCtrl(backendSrv, $rootScope, $timeout) {
          _classCallCheck(this, AuthsCtrl);

          this.$rootScope = $rootScope;
          this.$timeout = $timeout;
          this.backendSrv = backendSrv;
        }

        _createClass(AuthsCtrl, [{
          key: 'login',
          value: function login(datasource) {
            this.fetching = {};
            this.fetching.done = false;
            this.fetching.show = true;
            this.fetching.status = 'warning';
            this.fetching.title = 'Fetching auths...';

            //this.$rootScope.appEvent('alert-success', ['Authorization Cookie Received', '']);
            //this.$rootScope.appEvent('alert-error', ['Authorization Failed', '']);
            //this.$rootScope.appEvent('alert-warning', ['Fetching Authorizations', '']);

            var uri = this._getURI(datasource.jsonData);

            if (datasource.jsonData.basicAuths) {
              if (this.basicAuthUser && this.basicAuthPassword) {
                var authBlob = { username: this.basicAuthUser, password: this.basicAuthPassword };
                this.doPostLogin(uri, authBlob);
              } else {
                this.$rootScope.appEvent('alert-warning', [datasource.name, 'requires User/Password']);
                this.fetching.status = 'warning';
                this.fetching.title = 'User/Password required';

                return;
              }
            } else {
              this.doGetLogin(uri);
            }

            this.$timeout(function () {
              console.log('timeout complete');
              this.fetching.done = true;
              this.fetching.status = 'success';
              this.fetching.title = "Success";
            }.bind(this), 3000);
          }
        }, {
          key: 'doPostLogin',
          value: function doPostLogin(uri, authBlob) {
            console.log('post ' + uri + ' <- ' + JSON.stringify(authBlob));

            this.backendSrv.post(uri, authBlob).then(function (result) {
              this.handleLoginResult(result);
            }.bind(this));
          }
        }, {
          key: 'doGetLogin',
          value: function doGetLogin(uri) {
            console.log('get ' + uri);

            this.backendSrv.get(uri).then(function (result) {
              this.handleLoginResult(result);
            }.bind(this));
          }
        }, {
          key: 'handleLoginResult',
          value: function handleLoginResult(result) {
            console.log("Login Result:");
            console.log(result);
          }
        }, {
          key: '_getURI',
          value: function _getURI(jsonData) {
            return 'https://' + jsonData.timelyHost + ':' + jsonData.httpsPort + '/login';
          }
        }, {
          key: '_showResults',
          value: function _showResults() {}
        }]);

        return AuthsCtrl;
      }());

      _export('AuthsCtrl', AuthsCtrl);
    }
  };
});
//# sourceMappingURL=auths.js.map
