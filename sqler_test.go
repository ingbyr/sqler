package main

import (
	"github.com/stretchr/testify/assert"
	"sqler/pkg"
	"testing"
)

func TestLoadSchema(t *testing.T) {
	a := assert.New(t)
	printer = NewPrinter()
	cfg, _ := pkg.LoadConfigFromFile("jdbc.properties")
	s := NewSqler(cfg)

	s.loadSchema()
	a.NotNil(s)
}
