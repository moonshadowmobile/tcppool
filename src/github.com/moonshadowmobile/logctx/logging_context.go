/*=======================================================================*/
// logging_context.go
// Created: Sat Aug 09 04:10:06 PDT 2014 @507 /Internet Time/
// :mode=Google go:tabSize=3:indentSize=3:
//  Modified 2015.6.5: added map and stack.
// Purpose:
/*======================================================================*/
package logctx

import (
	"container/list"
	"errors"
	"fmt"
	"os"
	"sync"
)

import l "github.com/cihub/seelog"

type LoggingContext struct {
	Name     string
	Cls      string
	Usable   bool
	guard    sync.RWMutex
	logmap   map[string]*l.LoggerInterface
	logstack *list.List
	curr     string
}

const cls_logctx string = "LoggingContext"

var LoggingContextCount int

/*======================================================================*/
func (lc *LoggingContext) Tracef(lname string, f string, p ...interface{}) {
	var ok bool = true
	var pushed bool = false
	var e error
	if lc.curr != lname {
		e, ok = lc.Push(lname)
		if !ok {
			fmt.Printf("[%s] * Logging error: %s", lc.Cls, e)
		} else {
			pushed = true
		}
	}
	if ok {
		l.Tracef(f, p...)
	}
	if pushed {
		lc.Pop()
	}
}

/*======================================================================*/
func (lc *LoggingContext) Debugf(lname string, f string, p ...interface{}) {
	var ok bool = true
	var pushed bool = false
	var e error
	if lc.curr != lname {
		e, ok = lc.Push(lname)
		if !ok {
			fmt.Printf("[%s] * Logging error: %s", lc.Cls, e)
		} else {
			pushed = true
		}
	}
	if ok {
		l.Debugf(f, p...)
	}
	if pushed {
		lc.Pop()
	}
}

/*======================================================================*/
func (lc *LoggingContext) Infof(lname string, f string, p ...interface{}) {
	var ok bool = true
	var pushed bool = false
	var e error
	if lc.curr != lname {
		e, ok = lc.Push(lname)
		if !ok {
			fmt.Printf("[%s] * Logging error: %s", lc.Cls, e)
		} else {
			pushed = true
		}
	}
	if ok {
		l.Infof(f, p...)
	}
	if pushed {
		lc.Pop()
	}
}

/*======================================================================*/
func (lc *LoggingContext) Warnf(lname string, f string, p ...interface{}) error {
	var ok bool = true
	var pushed bool = false
	var err, e error
	if lc.curr != lname {
		e, ok = lc.Push(lname)
		if !ok {
			fmt.Printf("[%s] * Logging error: %s", lc.Cls, e)
		} else {
			pushed = true
		}
	}
	if ok {
		err = l.Warnf(f, p...)
	}
	if pushed {
		lc.Pop()
	}
	return err
}

/*======================================================================*/
func (lc *LoggingContext) Errorf(lname string, f string, p ...interface{}) error {
	var ok bool = true
	var pushed bool = false
	var err, e error
	if lc.curr != lname {
		e, ok = lc.Push(lname)
		if !ok {
			fmt.Printf("[%s] * Logging error: %s", lc.Cls, e)
		} else {
			pushed = true
		}
	}
	if ok {
		err = l.Errorf(f, p...)
	}
	if pushed {
		lc.Pop()
	}
	return err
}

/*======================================================================*/
func (lc *LoggingContext) Criticalf(lname string, f string, p ...interface{}) error {
	var ok bool = true
	var pushed bool = false
	var err, e error
	if lc.curr != lname {
		e, ok = lc.Push(lname)
		if !ok {
			fmt.Printf("[%s] * Logging error: %s", lc.Cls, e)
		} else {
			pushed = true
		}
	}
	if ok {
		err = l.Criticalf(f, p...)
	}
	if pushed {
		lc.Pop()
	}
	return err
}

/*======================================================================*/
func (lc *LoggingContext) Fatalf(lname string, f string, p ...interface{}) {
	var ok bool = true
	var pushed bool = false
	var e error
	if lc.curr != lname {
		e, ok = lc.Push(lname)
		if !ok {
			fmt.Printf("[%s] * Logging error: %s", lc.Cls, e)
		} else {
			pushed = true
		}
	}
	if ok {
		l.Criticalf(f, p...)
	}
	if pushed {
		lc.Pop()
	}
	os.Exit(1)
}

/*======================================================================*/
func (lc *LoggingContext) Panicf(lname string, f string, p ...interface{}) {
	var ok bool = true
	var pushed bool = false
	var e error
	if lc.curr != lname {
		e, ok = lc.Push(lname)
		if !ok {
			fmt.Printf("[%s] * Logging error: %s", lc.Cls, e)
		} else {
			pushed = true
		}
	}
	if ok {
		l.Criticalf(f, p...)
	}
	if pushed {
		lc.Pop()
	}
	s := fmt.Sprintf(f, p...)
	panic(s)
}

/*======================================================================*/
func (lc *LoggingContext) Flush() {
	l.Flush()
}

/*======================================================================*/
//  If second return arg == true, logging will continue,
//   regardless of error.
/*======================================================================*/
func (lc *LoggingContext) SwitchTo(name string) (error, bool) {
	if lc.curr == name {
		return nil, true
	}
	lc.guard.Lock()
	defer lc.guard.Unlock()
	logger := lc.logmap[name]
	if logger == nil {
		return errors.New(fmt.Sprintf("Logger \"%s\" not found. Still using \"%s\"", name, lc.curr)), true
	}
	lc.curr = name
	l.UseLogger(*logger)
	return nil, true
}

/*======================================================================*/
//  If second return arg == true, logging will continue,
//   regardless of error.
/*======================================================================*/
func (lc *LoggingContext) Pop() (error, bool) {
	if lc.curr == "" {
		return errors.New(fmt.Sprintf("Cannot pop context; no loggers have been added")), false
	}
	lc.guard.Lock()
	defer lc.guard.Unlock()
	if lc.logstack.Len() == 0 {
		return errors.New(fmt.Sprintf("Cannot pop context; logger stack empty. Still using \"%s\"", lc.curr)), true
	}
	name := lc.logstack.Remove(lc.logstack.Back()).(string)
	lc.curr = name
	logger := lc.logmap[name]
	l.UseLogger(*logger)
	return nil, true
}

/*======================================================================*/
//  If second return arg == true, logging will continue,
//   regardless of error.
/*======================================================================*/
func (lc *LoggingContext) Push(name string) (error, bool) {
	if lc.curr == "" {
		return errors.New(fmt.Sprintf("Cannot push context to \"%s\"; no loggers have been added", name)), false
	}
	lc.guard.Lock()
	defer lc.guard.Unlock()
	logger := lc.logmap[name]
	if logger == nil {
		return errors.New(fmt.Sprintf("Logger \"%s\" not found. Still using \"%s\"", name, lc.curr)), true
	}
	lc.logstack.PushBack(lc.curr)
	lc.curr = name
	l.UseLogger(*logger)
	return nil, true
}

/*======================================================================*/
func (lc *LoggingContext) Default() error {
	if lc.curr == "" {
		return errors.New(fmt.Sprintf("Cannot set default logger; no loggers have been added"))
	}
	lc.guard.RLock()
	logger := lc.logmap["default"]
	l.UseLogger(*logger)
	lc.guard.RUnlock()
	return nil
}

/*======================================================================*/
func (lc *LoggingContext) AddLogger(name string, lfpath string) error {
	logger, err := l.LoggerFromConfigAsFile(lfpath)
	if err != nil {
		return err
	}
	lc.guard.Lock()
	// first logger added will also be default/fallback
	defer lc.guard.Unlock()
	if len(lc.logmap) == 0 {
		lc.logmap["default"] = &logger
		lc.curr = name
		l.UseLogger(logger)
	}
	lc.logmap[name] = &logger
	return nil
}

/*======================================================================*/
func (lc *LoggingContext) AddLoggerFromString(name string, lcfg string) error {
	logger, err := l.LoggerFromConfigAsBytes([]byte(lcfg))
	if err != nil {
		return err
	}
	lc.guard.Lock()
	// first logger added will also be default/fallback
	defer lc.guard.Unlock()
	if len(lc.logmap) == 0 {
		lc.logmap["default"] = &logger
		lc.curr = name
		l.UseLogger(logger)
	}
	lc.logmap[name] = &logger
	return nil
}

/*======================================================================*/
func NewLoggingContext() *LoggingContext {
	var lc *LoggingContext = new(LoggingContext)
	lc.Cls = cls_logctx
	LoggingContextCount++
	lc.Name = fmt.Sprintf("%s_%d", "LoggingContext", LoggingContextCount)
	lc.logmap = make(map[string]*l.LoggerInterface)
	lc.logstack = list.New()
	lc.Usable = true
	return lc
}
