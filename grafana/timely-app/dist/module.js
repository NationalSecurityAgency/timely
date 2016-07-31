'use strict';

System.register(['./components/config', './components/metrics', './components/authspage', './components/websocket'], function (_export, _context) {
  "use strict";

  var TimelyAppConfigCtrl, MetricsPageCtrl, AuthsPageCtrl, WebsocketCtrl;
  return {
    setters: [function (_componentsConfig) {
      TimelyAppConfigCtrl = _componentsConfig.TimelyAppConfigCtrl;
    }, function (_componentsMetrics) {
      MetricsPageCtrl = _componentsMetrics.MetricsPageCtrl;
    }, function (_componentsAuthspage) {
      AuthsPageCtrl = _componentsAuthspage.AuthsPageCtrl;
    }, function (_componentsWebsocket) {
      WebsocketCtrl = _componentsWebsocket.WebsocketCtrl;
    }],
    execute: function () {
      _export('ConfigCtrl', TimelyAppConfigCtrl);

      _export('MetricsPageCtrl', MetricsPageCtrl);

      _export('AuthsPageCtrl', AuthsPageCtrl);

      _export('WebsocketPageCtrl', WebsocketCtrl);
    }
  };
});
//# sourceMappingURL=module.js.map
