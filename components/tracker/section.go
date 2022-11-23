package tracker

import (
	"github.com/Zondax/zindexer/components/connections/database/postgres"
	"sort"
)

// Section defines an interval of heights with inclusive boundaries [StartIdx, EndIdx]
type Section struct {
	StartIdx uint64
	EndIdx   uint64
}

type Sections = []Section

func (Section) TableName() string {
	return postgres.GetTableName("tracking")
}

func BuildSectionsFromSlice(heights *[]uint64) Sections {
	var sections Sections
	if heights == nil {
		return sections
	}

	for i := 0; i < len(*heights); i++ {
		sections = append(sections, Section{
			StartIdx: (*heights)[i],
			EndIdx:   (*heights)[i],
		})
	}

	return MergeSections(sections)
}

func BuildSliceFromSections(sections Sections) *[]uint64 {
	sections = MergeSections(sections)
	if len(sections) == 0 {
		return &[]uint64{}
	}

	maxCapacity := sections[len(sections)-1].EndIdx - sections[0].StartIdx
	if maxCapacity == 0 {
		// this section contains the same value for StartIdx and EndIdx, maxCapacity should be 1
		maxCapacity = 1
	}

	var a = make([]uint64, 0, maxCapacity)
	for _, section := range sections {
		for i := section.StartIdx; i <= section.EndIdx; i++ {
			a = append(a, i)
		}
	}

	return &a
}

func FindGapsInSections(sections Sections) *[]uint64 {
	// Merge sections and estimate max capacity
	sections = MergeSections(sections)
	if len(sections) < 2 {
		return &[]uint64{}
	}

	maxCapacity := sections[len(sections)-1].StartIdx - sections[0].EndIdx
	var missing = make([]uint64, 0, maxCapacity)

	for i := 1; i < len(sections); i++ {
		lastSection := sections[i-1]
		high := sections[i].StartIdx
		if high > 0 {
			high--
		}
		low := lastSection.EndIdx
		for j := high; j > low; j-- {
			missing = append(missing, j)
		}
	}

	// Return missing in desc order prioritizing newer blocks
	sort.Slice(missing, func(i, j int) bool {
		return missing[j] < missing[i]
	})

	return &missing
}

func MergeSections(sections Sections) Sections {
	var merged Sections

	if len(sections) > 1 {
		sort.Slice(sections, func(i, j int) bool {
			return sections[i].StartIdx < sections[j].StartIdx
		})
	}

	for _, section := range sections {
		if section.StartIdx > section.EndIdx {
			t := section
			section.StartIdx = t.EndIdx
			section.EndIdx = t.StartIdx
		}
		last := len(merged) - 1
		if last < 0 {
			merged = append(merged, section)
			continue
		}

		if section.StartIdx == merged[last].EndIdx+1 {
			merged[last].EndIdx = section.EndIdx
			continue
		}

		if section.StartIdx > merged[last].EndIdx {
			merged = append(merged, section)
			continue
		}

		if section.EndIdx > merged[last].EndIdx {
			merged[last].EndIdx = section.EndIdx
			continue
		}
	}

	return merged
}

// RemoveSections removes any sections included in (toRemove: Sections) that intersect with (sections: Sections)
func RemoveSections(sections, toRemove Sections) Sections {
	sectionsFlat := BuildSliceFromSections(sections)
	toRemoveFlat := BuildSliceFromSections(toRemove)

	resultFlat := make([]uint64, 0, len(*sectionsFlat))

	i := 0
	j := 0

	for i < len(*sectionsFlat) && j < len(*toRemoveFlat) {
		if (*sectionsFlat)[i] == (*toRemoveFlat)[j] {
			i++
			j++
			continue
		}

		if (*sectionsFlat)[i] < (*toRemoveFlat)[j] {
			resultFlat = append(resultFlat, (*sectionsFlat)[i])
			i++
		} else {
			j++
		}
	}

	resultFlat = append(resultFlat, (*sectionsFlat)[i:]...)
	result := BuildSectionsFromSlice(&resultFlat)

	return result
}
