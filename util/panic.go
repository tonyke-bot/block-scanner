package util

import (
	"bytes"
	"fmt"
	"runtime"

	"github.com/sirupsen/logrus"
)

func ExecuteWithRecover(logger *logrus.Entry, actionName string, action func() error) (err error) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}

		logger.WithField("panic", "r").WithField("stack", Stack(3)).Errorf("%v panics", actionName)
		err = fmt.Errorf("panic: %v", r)
	}()

	return action()
}

// Stack returns a nicely formatted Stack frame, skipping skip frames.
func Stack(skip int) string {
	buf := new(bytes.Buffer) // the returned data
	// As we loop, we open files and read them. These variables record the currently
	// loaded file.
	for i := skip; ; i++ { // Skip the expected number of frames
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		// Print this much at least.  If we can't find the source, it won't show.
		fmt.Fprintf(buf, "%s:%d (0x%x)\n", file, line, pc)
	}
	return buf.String()
}
