package pkg

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLoadConfigFromFile(t *testing.T) {
	a := assert.New(t)
	cfg, err := LoadConfigFromFile("../config.yml")
	a.NoError(err)
	a.Equal(cfg.CommandsConfig.CountSchemas, []string{"t1", "t2"})
}
