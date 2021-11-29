package version

import (
	"fmt"
	"runtime"
)

const Binary = "1.2.1"

func String(app string) string {
	return fmt.Sprintf("%s v%s (build w/%s)", app, Binary, runtime.Version())
}
