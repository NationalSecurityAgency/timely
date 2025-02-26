//go:build mage
// +build mage

package main

import (
	// mage:import
	build "github.com/grafana/grafana-plugin-sdk-go/build"
)

var callback = func(cfg build.Config) (build.Config, error) {
	cfg.OutputBinaryPath = "dist"
	cfg.PluginJSONPath = "src"
	cfg.RootPackagePath = "."
	return cfg, nil
}

var Unused = build.SetBeforeBuildCallback(callback)

// Default configures the default target.
var Default = build.BuildAll
