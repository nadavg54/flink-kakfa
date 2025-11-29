package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// FileConfig represents the configuration loaded from a file
type FileConfig struct {
	Broker   string   `json:"broker"`
	Topic    string   `json:"topic"`
	Interval string   `json:"interval"` // e.g., "2s", "500ms"
	WordList []string `json:"word_list"`
}

// LoadConfig loads configuration from a file, with environment variable overrides
// Environment variables take highest priority and override file config
func LoadConfig(configPath string) (Config, error) {
	config, err := loadConfigFromFile(configPath)
	if err != nil {
		return Config{}, err
	}

	config = applyConfigDefaults(config)
	config = applyEnvironmentVariableOverrides(config)

	return config, nil
}

// loadConfigFromFile loads configuration from a JSON file if the file exists
func loadConfigFromFile(configPath string) (Config, error) {
	var config Config

	if configPath == "" {
		return config, nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		// Config file is optional - can use env vars only
		if !os.IsNotExist(err) {
			return config, fmt.Errorf("failed to read config file: %w", err)
		}
		return config, nil
	}

	fileConfig, err := parseConfigFile(data)
	if err != nil {
		return config, err
	}

	return convertFileConfigToConfig(fileConfig)
}

// parseConfigFile parses JSON data into a FileConfig struct
func parseConfigFile(data []byte) (FileConfig, error) {
	var fileConfig FileConfig
	if err := json.Unmarshal(data, &fileConfig); err != nil {
		return fileConfig, fmt.Errorf("failed to parse config file: %w", err)
	}
	return fileConfig, nil
}

// convertFileConfigToConfig converts FileConfig to Config, parsing the interval duration
func convertFileConfigToConfig(fileConfig FileConfig) (Config, error) {
	config := Config{
		Broker:   fileConfig.Broker,
		Topic:    fileConfig.Topic,
		WordList: fileConfig.WordList,
	}

	if fileConfig.Interval != "" {
		interval, err := time.ParseDuration(fileConfig.Interval)
		if err != nil {
			return config, fmt.Errorf("invalid interval format: %w", err)
		}
		config.Interval = interval
	}

	return config, nil
}

// applyConfigDefaults applies default values to config fields that are not set
func applyConfigDefaults(config Config) Config {
	if config.Broker == "" {
		config.Broker = defaultBroker
	}
	if config.Topic == "" {
		config.Topic = defaultTopic
	}
	if config.Interval == 0 {
		config.Interval = 2 * time.Second
	}
	if config.WordList == nil {
		config.WordList = words
	}
	return config
}

// applyEnvironmentVariableOverrides applies environment variable overrides to config
func applyEnvironmentVariableOverrides(config Config) Config {
	if envBroker := os.Getenv("KAFKA_BROKER"); envBroker != "" {
		config.Broker = envBroker
	}
	if envTopic := os.Getenv("KAFKA_TOPIC"); envTopic != "" {
		config.Topic = envTopic
	}
	return config
}
