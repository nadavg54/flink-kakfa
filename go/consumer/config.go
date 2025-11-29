package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// FileConfig represents the configuration loaded from a file
type FileConfig struct {
	Broker  string `json:"broker"`
	Topic   string `json:"topic"`
	GroupID string `json:"group_id"`
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

	return convertFileConfigToConfig(fileConfig), nil
}

// parseConfigFile parses JSON data into a FileConfig struct
func parseConfigFile(data []byte) (FileConfig, error) {
	var fileConfig FileConfig
	if err := json.Unmarshal(data, &fileConfig); err != nil {
		return fileConfig, fmt.Errorf("failed to parse config file: %w", err)
	}
	return fileConfig, nil
}

// convertFileConfigToConfig converts FileConfig to Config
func convertFileConfigToConfig(fileConfig FileConfig) Config {
	return Config{
		Broker:  fileConfig.Broker,
		Topic:   fileConfig.Topic,
		GroupID: fileConfig.GroupID,
	}
}

// applyConfigDefaults applies default values to config fields that are not set
func applyConfigDefaults(config Config) Config {
	if config.Broker == "" {
		config.Broker = defaultBroker
	}
	if config.Topic == "" {
		config.Topic = defaultTopic
	}
	if config.GroupID == "" {
		config.GroupID = "word-count-consumer-group"
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
	if envGroupID := os.Getenv("KAFKA_GROUP_ID"); envGroupID != "" {
		config.GroupID = envGroupID
	}
	return config
}

