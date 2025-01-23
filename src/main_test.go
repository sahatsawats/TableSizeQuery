package main

import (
	"testing"
	"reflect"
)

func TestNormalReadStringToList(t *testing.T) {
	target := []string{"system1", "system2", "system3"}
	output := readStringToList("system1,system2,system3")

	if !reflect.DeepEqual(target, output) {
		t.Fatalf("readStringToList output: %v\nDoes not match expect: %v", output, target)
	}
}

func TestSingleQoutesReadStringToList(t *testing.T) {
	target := []string{"system1", "system2", "system3"}
	output := readStringToList("'system1','system2','system3'")

	if !reflect.DeepEqual(target, output) {
		t.Fatalf("readStringToList output: %v\nDoes not match expect: %v", output, target)
	}
}

func TestTrimSingleQoutesReadStringToList(t *testing.T) {
	target := []string{"system1", "system2", "system3"}
	output := readStringToList("'system1', 'system2', 'system3'")

	if !reflect.DeepEqual(target, output) {
		t.Fatalf("readStringToList output: %v\nDoes not match expect: %v", output, target)
	}
}

func TestBuildQueryStatement(t *testing.T) {
	sliceOfExcludeOwner := []string{"system1", "system2"}
	target := "SELECT owner, table_name FROM dba_tables WHERE owner NOT IN ('system1','system2')"
	output := buildQueryStatement(sliceOfExcludeOwner)

	if output != target {
		t.Fatalf("BuildQueryStatement output: %s\nDoes not match expect: %s", output, target)
	}
}