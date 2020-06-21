//+build mage

package main

import (
	"fmt"

	// mage:import
	_ "github.com/grafana/grafana-plugin-sdk-go/build"
)

// CheckCheck builds production back-end components.
func CheckCheck() {
	fmt.Println("hello! TODO.... (the other commands are imported)")
}

// Default configures the default target.
var Default = CheckCheck
