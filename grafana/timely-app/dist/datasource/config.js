'use strict';

System.register(['lodash'], function (_export, _context) {
  "use strict";

  var _, DatasourceConfigCtrl;

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
      _export('DatasourceConfigCtrl', DatasourceConfigCtrl = function DatasourceConfigCtrl() {
        _classCallCheck(this, DatasourceConfigCtrl);

        this.current.jsonData = this.current.jsonData || {};
        this.current.access = 'direct';
        this.current.basicAuth = false;
        this.current.withCredentials = true;
        this.current.suggestHost = 'localhost';
      });

      _export('DatasourceConfigCtrl', DatasourceConfigCtrl);

      DatasourceConfigCtrl.templateUrl = 'datasource/partials/config.html';
    }
  };
});
//# sourceMappingURL=config.js.map
