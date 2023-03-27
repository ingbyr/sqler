package main

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoadSchema(t *testing.T) {
	a := assert.New(t)
	printer = NewPrinter()
	cfg := LoadConfig("jdbc.properties")
	s := NewSqler(cfg)

	s.loadSchema()
	a.NotNil(s)
}
