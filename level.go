package main

//go:generate stringer -type Level -linecomment
type Level uint

const (
	Debug Level = iota // [DEBUG]
	Info               // [INFO]
	Warn               // [WARN]
	Error              // [ERROR]
)
