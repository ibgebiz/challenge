package domain

import (
	"fmt"
	"regexp"
)

var tmplVar = regexp.MustCompile(`{{\s*(\w+)\s*}}`)

// Render substitutes {{variable}} placeholders in body using vars. It returns an
// error if any referenced variable is missing from vars.
func Render(body string, vars map[string]string) (string, error) {
	var missing string
	out := tmplVar.ReplaceAllStringFunc(body, func(m string) string {
		key := tmplVar.FindStringSubmatch(m)[1]
		v, ok := vars[key]
		if !ok {
			missing = key
			return m
		}
		return v
	})
	if missing != "" {
		return "", fmt.Errorf("%w: missing template variable %q", ErrValidation, missing)
	}
	return out, nil
}
