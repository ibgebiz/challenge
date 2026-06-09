package domain

import "testing"

func TestRender(t *testing.T) {
	out, err := Render("Hello {{name}}, code {{code}}", map[string]string{"name": "Ada", "code": "42"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "Hello Ada, code 42" {
		t.Fatalf("got %q", out)
	}
}

func TestRender_MissingVariable(t *testing.T) {
	if _, err := Render("Hi {{missing}}", map[string]string{}); err == nil {
		t.Fatal("expected missing-var error")
	}
}

func TestRender_HandlesWhitespace(t *testing.T) {
	out, err := Render("Hi {{ name }}", map[string]string{"name": "Bo"})
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if out != "Hi Bo" {
		t.Fatalf("got %q", out)
	}
}
