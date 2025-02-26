import { DataSourcePlugin } from '@grafana/data';
import { TimelyDataSource } from './TimelyDataSource';
import { ConfigEditor } from './components/ConfigEditor';
import { QueryEditor } from './components/QueryEditor';
import { TimelyDataSourceOptions, TimelyQueryForm } from './types';
import 'static/css/timely-bootstrap.css';
import 'static/css/timely.css';
import { loadPluginCss } from '@grafana/runtime';

loadPluginCss({
  dark: 'plugins/nsa-timely-datasource/styles/dark.css',
  light: 'plugins/nsa-timely-datasource/styles/light.css',
});

export const plugin = new DataSourcePlugin<TimelyDataSource, TimelyQueryForm, TimelyDataSourceOptions>(TimelyDataSource)
  .setConfigEditor(ConfigEditor)
  .setQueryEditor(QueryEditor);
