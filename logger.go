package zanredisdb

import (
	"fmt"
	"log"
	"os"
	"sync/atomic"
)

type Logger interface {
	Output(depth int, s string)
	OutputErr(depth int, s string)
	Flush()
}

type LevelLogger struct {
	Logger Logger
	level  int32
}

type SimpleLogger struct {
	logger *log.Logger
}

func NewSimpleLogger() *SimpleLogger {
	return &SimpleLogger{
		logger: log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile|log.Lmicroseconds),
	}
}

func header(lvl, msg string) string {
	return fmt.Sprintf("%s: %s", lvl, msg)
}

func (self *SimpleLogger) Output(depth int, s string) {
	self.logger.Output(depth+1, s)
}

func (self *SimpleLogger) OutputErr(depth int, s string) {
	self.logger.Output(depth+1, header("ERR", s))
}

func (self *SimpleLogger) Flush() {
}

func NewLevelLogger(l int32, logger Logger) *LevelLogger {
	return &LevelLogger{Logger: logger, level: l}
}

func (self *LevelLogger) Flush() {
	if self.Logger != nil {
		self.Logger.Flush()
	}
}

func (self *LevelLogger) SetLevel(l int32) {
	atomic.StoreInt32(&self.level, l)
}

func (self *LevelLogger) Level() int32 {
	return self.level
}

func (self *LevelLogger) Infof(f string, args ...interface{}) {
	if self.Logger != nil && self.level > 0 {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

// used only for wrap call (for other logger interface)
func (self *LevelLogger) Printf(f string, args ...interface{}) {
	if self.Logger != nil {
		self.Logger.Output(3, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Infoln(f string) {
	if self.Logger != nil && self.level > 0 {
		self.Logger.Output(2, f)
	}
}

func (self *LevelLogger) Debugf(f string, args ...interface{}) {
	if self.Logger != nil && self.level > 1 {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Detailf(f string, args ...interface{}) {
	if self.Logger != nil && self.level > 2 {
		self.Logger.Output(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Warningf(f string, args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Warningln(f string) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, f)
	}
}

func (self *LevelLogger) Errorf(f string, args ...interface{}) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, fmt.Sprintf(f, args...))
	}
}

func (self *LevelLogger) Errorln(f string) {
	if self.Logger != nil {
		self.Logger.OutputErr(2, f)
	}
}

var levelLog = NewLevelLogger(1, NewSimpleLogger())

// should call only once before any proxy started.
func SetLogger(level int32, l Logger) {
	levelLog.Logger = l
	levelLog.SetLevel(level)
}
