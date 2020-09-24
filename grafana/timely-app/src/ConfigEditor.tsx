import { DataSourcePluginOptionsEditorProps } from "@grafana/data";
import { LegacyForms } from "@grafana/ui";
import { ChangeEvent, PureComponent } from "react";
import { TimelyDataSourceOptions } from "./types";
import React from "react";

const { FormField, Switch } = LegacyForms;

interface Props
  extends DataSourcePluginOptionsEditorProps<TimelyDataSourceOptions> {}

interface State {}

export class ConfigEditor extends PureComponent<Props, State> {
  constructor(props: Props) {
    super(props);
  }

  onTimelyHostChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      timelyHost: event.target.value
    };
    onOptionsChange({ ...options, jsonData });
  };

  onHttpsPortChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      httpsPort: event.target.value
    };
    onOptionsChange({ ...options, jsonData });
  };

  onUseOAuthChange = (event: React.SyntheticEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      oauthPassThru: event.currentTarget.checked
    };
    onOptionsChange({ ...options, jsonData });
  };

  onUseClientCertWhenOAuthMissingChange = (
    event: React.SyntheticEvent<HTMLInputElement>
  ) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      useClientCertWhenOAuthMissing: event.currentTarget.checked
    };
    onOptionsChange({ ...options, jsonData });
  };

  onClientCertificatePathChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      clientCertificatePath: event.target.value
    };
    onOptionsChange({ ...options, jsonData });
  };

  onClientKeyPathChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      clientKeyPath: event.target.value
    };
    onOptionsChange({ ...options, jsonData });
  };

  onCertificateAuthorityPathChange = (event: ChangeEvent<HTMLInputElement>) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      certificateAuthorityPath: event.target.value
    };
    onOptionsChange({ ...options, jsonData });
  };

  onAllowInsecureSslChange = (
    event: React.SyntheticEvent<HTMLInputElement>
  ) => {
    const { onOptionsChange, options } = this.props;
    const jsonData = {
      ...options.jsonData,
      allowInsecureSsl: event.currentTarget.checked
    };
    onOptionsChange({ ...options, jsonData });
  };

  render() {
    const { options } = this.props;
    const { jsonData } = options;

    return (
      <div className="gf-form-group">
        <div className="gf-form">
          <FormField
            label={"Hostname or IP"}
            labelWidth={10}
            inputWidth={20}
            disabled={false}
            onChange={this.onTimelyHostChange}
            tooltip={"Hostname or IP of Timely Server"}
            value={jsonData.timelyHost || ""}
            placeholder={"localhost"}
          />
        </div>
        <div className="gf-form">
          <FormField
            label={"Https port"}
            labelWidth={10}
            inputWidth={20}
            onChange={this.onHttpsPortChange}
            value={jsonData.httpsPort || ""}
            placeholder={"4243"}
          />
        </div>
        <div className="gf-form">
          <Switch
            label={"Use OAuth token"}
            labelClass="width-10"
            tooltipPlacement={"top"}
            tooltip={"Send OAuth token if available"}
            checked={jsonData.oauthPassThru || false}
            onChange={this.onUseOAuthChange}
          />
        </div>
        <div className="gf-form">
          <Switch
            label={"Client cert for users"}
            labelClass="width-10"
            tooltipPlacement={"top"}
            tooltip={"If user is missing OAuth token, use client cert instead"}
            checked={jsonData.useClientCertWhenOAuthMissing || false}
            onChange={this.onUseClientCertWhenOAuthMissingChange}
          />
        </div>
        <div className="gf-form">
          <FormField
            label={"Client cert path"}
            labelWidth={10}
            inputWidth={40}
            disabled={false}
            onChange={this.onClientCertificatePathChange}
            tooltip={"Full path to client certificate"}
            value={jsonData.clientCertificatePath || ""}
          />
        </div>
        <div className="gf-form">
          <FormField
            label={"Client key path"}
            labelWidth={10}
            inputWidth={40}
            disabled={false}
            onChange={this.onClientKeyPathChange}
            tooltip={"Full path to client key"}
            value={jsonData.clientKeyPath || ""}
          />
        </div>
        <div className="gf-form">
          <FormField
            label={"CA path"}
            labelWidth={10}
            inputWidth={40}
            disabled={false}
            onChange={this.onCertificateAuthorityPathChange}
            tooltip={"Full path to certificate authority"}
            value={jsonData.certificateAuthorityPath || ""}
          />
        </div>
        <div className="gf-form">
          <Switch
            label={"Allow insecure ssl"}
            labelClass="width-10"
            tooltipPlacement={"top"}
            tooltip={
              "Allow ssl connection with unverified or self-signed certificates"
            }
            checked={jsonData.allowInsecureSsl || false}
            onChange={this.onAllowInsecureSslChange}
          />
        </div>

        <div className="gf-form">
          <p>
            <a
              className="external-link"
              target="_blank"
              href=""
              onClick={() =>
                window.open(
                  "https://" +
                    jsonData.timelyHost +
                    ":" +
                    jsonData.httpsPort +
                    "/version",
                  "_blank"
                )
              }
            >
              Open a new browser tab/window to test https access
            </a>
          </p>
        </div>
      </div>
    );
  }
}
