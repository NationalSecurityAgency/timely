'use strict';

System.register(['./components/config', './components/metrics', './components/authspage'], function (_export, _context) {
  "use strict";

  var TimelyAppConfigCtrl, MetricsPageCtrl, AuthsPageCtrl;
  return {
    setters: [function (_componentsConfig) {
      TimelyAppConfigCtrl = _componentsConfig.TimelyAppConfigCtrl;
    }, function (_componentsMetrics) {
      MetricsPageCtrl = _componentsMetrics.MetricsPageCtrl;
    }, function (_componentsAuthspage) {
      AuthsPageCtrl = _componentsAuthspage.AuthsPageCtrl;
    }],
    execute: function () {
      _export('ConfigCtrl', TimelyAppConfigCtrl);

      _export('MetricsPageCtrl', MetricsPageCtrl);

      _export('AuthsPageCtrl', AuthsPageCtrl);
    }
  };
});
//# sourceMappingURL=module.js.map
