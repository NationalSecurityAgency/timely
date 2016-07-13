import {TimelyDatasource} from './datasource';
import {TimelyQueryCtrl} from './query_ctrl';
import {DatasourceConfigCtrl} from './config';

class TimelyAnnotationsQueryCtrl {}
TimelyAnnotationsQueryCtrl.templateUrl = 'components/annotations.editor.html';

export {
  TimelyDatasource as Datasource,
  TimelyQueryCtrl as QueryCtrl,
  DatasourceConfigCtrl as ConfigCtrl,
  TimelyAnnotationsQueryCtrl as AnnotationsQueryCtrl
};
