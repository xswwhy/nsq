package lg

import (
	"fmt"
	"log"
	"os"
	"strings"
)

const (
	DEBUG = LogLevel(1)
	INFO  = LogLevel(2)
	WARN  = LogLevel(3)
	ERROR = LogLevel(4)
	FATAL = LogLevel(5)
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

type Logger interface {
	Output(maxdepth int, s string) error
}

// 实现Logger接口的空日志,用于测试用
type NilLogger struct{}

func (l NilLogger) Output(maxdepth int, s string) error {
	return nil
}

type LogLevel int

// 获取日志等级
func (l *LogLevel) Get() interface{} {
	return *l
}

// 设置日志等级
func (l *LogLevel) Set(s string) {
	ParseLogLevel(s)
}

// 获取当前日志等级对应的string
func (l *LogLevel) String() string {
	switch *l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARNING"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	}
	return "invalid"
}

// 获取string对应的日志等级
func ParseLogLevel(levelstr string) (LogLevel, error) {
	switch strings.ToLower(levelstr) {
	case "debug":
		return DEBUG, nil
	case "info":
		return INFO, nil
	case "warn":
		return WARN, nil
	case "error":
		return ERROR, nil
	case "fatal":
		return FATAL, nil
	}
	return 0, fmt.Errorf("invalid log level '%s'  (debug, info, warn, error, fatal)", levelstr)
}

// 记录日志
func Logf(logger Logger, cfgLevel LogLevel, msgLevel LogLevel, f string, args ...interface{}) {
	if cfgLevel > msgLevel {
		return
	}
	logger.Output(3, fmt.Sprintf(msgLevel.String()+": "+f, args...))
}

// 记录Fatal日志
func LogFatal(prefix string, f string, args ...interface{}) {
	logger := log.New(os.Stderr, prefix, log.Ldate|log.Ltime|log.Lmicroseconds) // 标准库log中Logger也实现了Output
	Logf(logger, FATAL, FATAL, f, args...)
	os.Exit(1)
}
