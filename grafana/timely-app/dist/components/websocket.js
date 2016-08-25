'use strict';

System.register(['plugins/timely-app/components/authspage'], function (_export, _context) {
  "use strict";

  var AuthsPageCtrl, _createClass, WebsocketCtrl;

  function _classCallCheck(instance, Constructor) {
    if (!(instance instanceof Constructor)) {
      throw new TypeError("Cannot call a class as a function");
    }
  }

  return {
    setters: [function (_pluginsTimelyAppComponentsAuthspage) {
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

      _export('WebsocketCtrl', WebsocketCtrl = function () {
        function WebsocketCtrl($scope, $timeout, backendSrv, datasourceSrv) {
          _classCallCheck(this, WebsocketCtrl);

          this.$scope = $scope;
          this.$timeout = $timeout;

          var actrl = new AuthsPageCtrl(backendSrv, datasourceSrv);

          var datasources = actrl.getTimelyDatasources();
          this.datasource = datasources[0].jsonData;

          this.websocketSupported = false;
          if (window.WebSocket) {
            this.websocketSupported = true;
          }
          this.metric_count = 0;
          this.metric = { 'metric': 'empty',
            'tags': [],
            'timestamp': 0,
            'value': 0.0
          };

          this.fetching = false;
        }

        _createClass(WebsocketCtrl, [{
          key: 'start',
          value: function start(metric) {
            this.fetching = true;
            var addr = this._getWebsocketAddress();
            this.sock = new WebSocket(addr);
            this.sock.onmessage = this._handleMessage.bind(this);
            this.sock.onopen = this._handleOpen.bind(this);
            this.sock.onclose = this._handleClose.bind(this);
          }
        }, {
          key: 'stop',
          value: function stop(metric) {
            this.fetching = false;
            this.sock.close();
          }
        }, {
          key: '_handleMessage',
          value: function _handleMessage(message) {
            //console.log('handleMessage');
            var metric = JSON.parse(message.data);

            this.metric.metric = metric.metric;
            this.metric.timestamp = metric.timestamp;
            this.metric.value = metric.value;
            this.metric.tags = metric.tags;
            this.metric_count = this.metric_count + 1;
            this.$scope.$digest();
            //if( this.metrics.length >= 16){
            //    this.metrics.shift();
            //}
            //this.$scope.$apply( function(){
            //        this.metrics.push(metric);
            //        this.metric_count = this.metric_count + 1;
            //        }.bind(this));
          }
        }, {
          key: '_handleOpen',
          value: function _handleOpen() {
            var create = {
              "operation": "create",
              "subscriptionId": "12345"
            };
            this.sock.send(JSON.stringify(create));

            var m1 = {
              "operation": "add",
              "subscriptionId": "12345",
              "metric": "sys.cpu.user"
            };
            this.sock.send(JSON.stringify(m1));

            //    var m2 = {
            //      "operation" : "add",
            //      "subscriptionId" : "12345",
            //      "metric" : "timely.keys.metric.inserted"
            //    }
            //    this.sock.send(JSON.stringify(m2));
          }
        }, {
          key: '_handleClose',
          value: function _handleClose() {
            console.log("Websocket Closed");
          }
        }, {
          key: '_getWebsocketAddress',
          value: function _getWebsocketAddress() {
            return "wss://" + this.datasource.timelyHost + ":" + this.datasource.wsPort + "/websocket";
          }
        }]);

        return WebsocketCtrl;
      }());

      _export('WebsocketCtrl', WebsocketCtrl);

      WebsocketCtrl.templateUrl = 'components/websocket.html';

      // example metric message
      //{
      //  "metric" : "sys.cpu.user",
      //  "timestamp":1469028728091,
      //  "value":1.0,
      //  "tags":
      //  [
      //    {
      //      "key":"rack",
      //      "value":"r1"
      //    },{
      //      "key":"tag3",
      //      "value":"value3"
      //    },{
      //      "key":"tag4",
      //      "value":"value4"
      //    }
      //  ],
      //  "subscriptionId":"<unique id>"
      //}
    }
  };
});
//# sourceMappingURL=websocket.js.map
