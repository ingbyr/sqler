// Code generated by "stringer -type Level -linecomment"; DO NOT EDIT.

package main

import "strconv"

func _() {
	// An "invalid array index" compiler error signifies that the constant values have changed.
	// Re-run the stringer command to generate them again.
	var x [1]struct{}
	_ = x[Debug-0]
	_ = x[Info-1]
	_ = x[Warn-2]
	_ = x[Error-3]
}

const _Level_name = "[DEBUG][INFO][WARN][ERROR]"

var _Level_index = [...]uint8{0, 7, 13, 19, 26}

func (i Level) String() string {
	if i >= Level(len(_Level_index)-1) {
		return "Level(" + strconv.FormatInt(int64(i), 10) + ")"
	}
	return _Level_name[_Level_index[i]:_Level_index[i+1]]
}