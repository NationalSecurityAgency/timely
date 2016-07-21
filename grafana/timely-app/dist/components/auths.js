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
          }
        }, {
          key: 'doPostLogin',
          value: function doPostLogin(uri, authBlob) {
            this.doXHR('POST', uri, authBlob);
          }
        }, {
          key: 'doGetLogin',
          value: function doGetLogin(uri) {
            this.doXHR('GET', uri);
          }
        }, {
          key: 'doXHR',
          value: function doXHR(verb, uri, data) {
            if (!data) {
              data = "";
            }
            var r = new XMLHttpRequest();
            r.open(verb, uri, true);
            r.withCredentials = true;
            r.setRequestHeader('Content-Type', 'application/json');
            r.onreadystatechange = function () {
              this.handleLoginResult(r);
            }.bind(this);
            r.send(JSON.stringify(data));
          }
        }, {
          key: 'handleLoginResult',
          value: function handleLoginResult(response) {
            // wait for readyState to be '4 => done'
            if (response.readyState != 4) return;

            this.fetching.done = true;

            switch (response.status) {
              case 200:
                var cookie = (document.cookie.match(/^(?:.*;)?TSESSIONID=([^;]+)(?:.*)?$/) || [, null])[1];
                console.log(cookie);
                console.log(document.cookie);

                this.fetching.status = 'success';
                this.fetching.title = 'Login successful';
                this.$rootScope.appEvent('alert-success', ['Success', '']);
                break;
              default:
                this.fetching.status = 'warning';
                this.fetching.title = response.statusText;
                this.$rootScope.appEvent('alert-warning', [response.status + ': ' + response.statusText, '']);
                break;
            }

            if (r.readyState != 4 || r.status != 200) return;
            alert("Success: " + r.getResponseHeader('Set-Cookie'));
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
