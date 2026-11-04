package main

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		zerolog     zerolog.Level
		logrusLevel logrus.Level
		wantErr     bool
	}{
		{name: "info", value: "info", zerolog: zerolog.InfoLevel, logrusLevel: logrus.InfoLevel},
		{name: "warning alias", value: "warning", zerolog: zerolog.WarnLevel, logrusLevel: logrus.WarnLevel},
		{name: "critical alias", value: "critical", zerolog: zerolog.FatalLevel, logrusLevel: logrus.FatalLevel},
		{name: "disabled alias", value: "off", zerolog: zerolog.Disabled, logrusLevel: logrus.PanicLevel},
		{name: "case and whitespace", value: " WARN ", zerolog: zerolog.WarnLevel, logrusLevel: logrus.WarnLevel},
		{name: "invalid", value: "unknown", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zerologLevel, logrusLevel, err := parseLogLevel(tt.value)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.zerolog, zerologLevel)
			require.Equal(t, tt.logrusLevel, logrusLevel)
		})
	}
}

func TestLogLevelWriterKeepsStartupMessagesAndFiltersInfo(t *testing.T) {
	var output bytes.Buffer
	writer := logLevelWriter{writer: &output, level: zerolog.WarnLevel}

	startup := []byte(`{"level":"info","time":"now","message":"Starting provider *docker.Provider"}` + "\n")
	n, err := writer.WriteLevel(zerolog.InfoLevel, startup)
	require.NoError(t, err)
	require.Equal(t, len(startup), n)
	require.Contains(t, output.String(), `"message":"Starting provider *docker.Provider"`)
	require.NotContains(t, output.String(), `"level":"info"`)

	info := []byte(`{"level":"info","message":"refreshing configuration"}` + "\n")
	n, err = writer.WriteLevel(zerolog.InfoLevel, info)
	require.NoError(t, err)
	require.Equal(t, len(info), n)
	require.NotContains(t, output.String(), "refreshing configuration")

	warn := []byte(`{"level":"warn","message":"warning"}` + "\n")
	_, err = writer.WriteLevel(zerolog.WarnLevel, warn)
	require.NoError(t, err)
	require.Contains(t, output.String(), `"message":"warning"`)
}
