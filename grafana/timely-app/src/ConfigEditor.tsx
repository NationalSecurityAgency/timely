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
            label={"HTTPS Port"}
            labelWidth={10}
            inputWidth={20}
            onChange={this.onHttpsPortChange}
            value={jsonData.httpsPort || ""}
            placeholder={"4243"}
          />
        </div>
        <div className="gf-form">
          <Switch
            label={"Use OAuth Token"}
            labelClass="width-10"
            tooltipPlacement={"top"}
            checked={jsonData.oauthPassThru || false}
            onChange={this.onUseOAuthChange}
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
