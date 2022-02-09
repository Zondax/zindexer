package tracker

import (
	"fmt"
	"reflect"
	"testing"
)

func TestTracker_MergeSections(t *testing.T) {
	tests := []struct {
		sections Sections
		want     Sections
	}{
		{
			Sections{{1, 3}, {2, 6}, {8, 10}, {15, 18}},
			Sections{{1, 6}, {8, 10}, {15, 18}},
		},
		{
			Sections{{1, 4}, {4, 5}},
			Sections{{1, 5}},
		},
		{
			Sections{{1, 2}},
			Sections{{1, 2}},
		},
		{
			Sections{{8, 7}, {2, 1}},
			Sections{{1, 2}, {7, 8}},
		},
		{
			Sections{},
			nil,
		},
		{
			Sections{{7, 10}, {3, 4}, {2, 5}},
			Sections{{2, 5}, {7, 10}},
		},
		{
			Sections{{1, 3}, {6, 8}, {8, 10}, {10, 15}, {15, 18}, {18, 20}},
			Sections{{1, 3}, {6, 20}},
		},
		{
			Sections{{1, 1}, {2, 2}},
			Sections{{1, 2}},
		},
	}

	for _, tt := range tests {
		got := mergeSections(tt.sections)
		fmt.Println(tt.sections, " ->", got)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("got: %v, want: %v", got, tt.want)
		}
	}
}

func TestTracker_BuildSectionsFromSlice(t *testing.T) {
	tests := []struct {
		heights []uint64
		want    Sections
	}{
		{
			[]uint64{1, 2, 3, 6, 8},
			Sections{{1, 3}, {6, 6}, {8, 8}},
		},
		{
			[]uint64{1, 2, 3, 4, 5, 6, 9, 10, 11, 13},
			Sections{{1, 6}, {9, 11}, {13, 13}},
		},
		{
			[]uint64{4, 5, 6, 9, 10, 11, 13, 1, 2, 3},
			Sections{{1, 6}, {9, 11}, {13, 13}},
		},
		{
			[]uint64{1},
			Sections{{1, 1}},
		},
	}

	for _, tt := range tests {
		got := buildSectionsFromSlice(&tt.heights)
		fmt.Println(tt.heights, " ->", got)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("got: %v, want: %v", got, tt.want)
		}
	}
}

func TestTracker_BuildSliceFromSection(t *testing.T) {
	tests := []struct {
		section Sections
		want    *[]uint64
	}{
		{
			Sections{{1, 3}, {6, 6}, {8, 8}},
			&[]uint64{1, 2, 3, 6, 8},
		},
		{

			Sections{{1, 6}, {9, 11}, {13, 13}},
			&[]uint64{1, 2, 3, 4, 5, 6, 9, 10, 11, 13},
		},
		{

			Sections{{1, 1}},
			&[]uint64{1},
		},
	}

	for _, tt := range tests {
		got := buildSliceFromSections(tt.section)
		fmt.Println(tt.section, " ->", got)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("got: %v, want: %v", got, tt.want)
		}
	}
}

func TestTracker_FindGapsInSection(t *testing.T) {
	tests := []struct {
		sections Sections
		want     *[]uint64
	}{
		{
			Sections{{1, 3}, {2, 6}, {8, 10}, {15, 18}},
			&[]uint64{14, 13, 12, 11, 7},
		},
		{
			Sections{{0, 0}, {5, 5}},
			&[]uint64{4, 3, 2, 1},
		},
		{
			Sections{{1, 1}, {1, 1}},
			&[]uint64{},
		},
		{
			Sections{{2, 3}, {0, 0}},
			&[]uint64{1},
		},
	}

	for _, tt := range tests {
		got := findGapsInSections(tt.sections)
		fmt.Println(tt.sections, " ->", got)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("got: %v, want: %v", got, tt.want)
		}
	}
}

func TestTracer_RemoveSection(t *testing.T) {
	type test struct {
		sections Sections
		toRemove Sections
		want     Sections
	}

	tests := []test{
		{
			Sections{{StartIdx: 0, EndIdx: 20}},
			Sections{{StartIdx: 1, EndIdx: 1}, {StartIdx: 4, EndIdx: 7},
				{StartIdx: 18, EndIdx: 22}, {StartIdx: 19, EndIdx: 19}},
			Sections{{StartIdx: 0, EndIdx: 0}, {StartIdx: 2, EndIdx: 3},
				{StartIdx: 8, EndIdx: 17}},
		},
		{
			Sections{{StartIdx: 0, EndIdx: 2}, {StartIdx: 5, EndIdx: 5},
				{StartIdx: 8, EndIdx: 50}, {StartIdx: 52, EndIdx: 100}},
			Sections{{StartIdx: 1, EndIdx: 5}, {StartIdx: 10, EndIdx: 22},
				{StartIdx: 7, EndIdx: 7}, {StartIdx: 60, EndIdx: 150}, {StartIdx: 130, EndIdx: 150}},
			Sections{{StartIdx: 0, EndIdx: 0}, {StartIdx: 8, EndIdx: 9},
				{StartIdx: 23, EndIdx: 50}, {StartIdx: 52, EndIdx: 59}},
		},
	}

	for _, test := range tests {
		result := RemoveSections(test.sections, test.toRemove)
		fmt.Println(test.sections, "-> ", result)
		if !reflect.DeepEqual(result, test.want) {
			t.Errorf("got: %v, want: %v", result, test.want)
		}
	}
}
