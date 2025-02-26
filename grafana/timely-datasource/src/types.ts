import { DataSourceJsonData, ScopedVars } from '@grafana/data';
import { DataQuery } from '@grafana/schema';

export interface TimelyQueryForm extends DataQuery {
  metric?: string;
  alias?: string;
  aggregator?: string;
  disableDownsampling?: boolean;
  downsampleInterval?: string;
  downsampleAggregator?: string;
  tags: { [key: string]: string };
  datasourceTags: string[];
  shouldComputeRate?: boolean;
  rateInterval?: string;
  isCounter?: boolean;
  counterMax?: string;
  counterResetValue?: string;
  downsampleFillPolicy?: string;
  tsuids?: string[];
  scopedVars?: ScopedVars;
}

/**
 * These are options configured for each DataSource instance
 */
export interface TimelyDataSourceOptions extends DataSourceJsonData {
  timelyHost: string;
  httpsPort: number;
  oauthPassThru: boolean;
  useClientCertWhenOAuthMissing: boolean;
  clientCertificatePath: string;
  clientKeyPath: string;
  certificateAuthorityPath: string;
  allowInsecureSsl: boolean;
  datasourceTags: { [key: string]: string };
}

export interface TimelyErrors {
  downsample?: string;
  rateInterval?: string;
}

export interface SearchLookupResults {
  tags: { [key: string]: string };
  metric: string;
  tsuid: string;
}

export interface SearchLookupResponse {
  type: string;
  metric: string;
  tags: { [key: string]: string };
  limit: number;
  time: number;
  totalResults: number;
  results: SearchLookupResults[];
}
