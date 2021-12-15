package util

import "os"

func GetEnvOrDefault(env, defaultValue string) string {
	if value := os.Getenv(env); value != "" {
		return value
	}

	return defaultValue
}
