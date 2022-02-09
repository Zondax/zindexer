package tracker

import (
	"sync"

	"go.uber.org/zap"
	"gorm.io/gorm"
)

type SectionId struct {
	Sections
	IndexerId string
}

type DbSection struct {
	Section
	IndexerId string
}

type DbSections = []DbSection

const (
	NoReturnLimit = 0
	WipStr        = "_wip"
)

var updateMutex sync.Mutex

func UpdateAndRemoveWipHeights(heights *[]uint64, id string, dbConn *gorm.DB) error {
	// Track these heights
	err := UpdateTrackedHeights(heights, id, dbConn)
	if err != nil {
		return err
	}

	// Remove WIP heights
	err = UpdateInProgressHeight(false, heights, id, dbConn)
	if err != nil {
		return err
	}

	return nil
}

func UpdateTrackedHeights(heights *[]uint64, id string, db *gorm.DB) error {
	updateMutex.Lock()
	defer updateMutex.Unlock()

	// Get current sections stored on DB
	sectionId, err := readDb(id, db)
	dbSections := sectionId.Sections
	if err != nil {
		zap.S().Errorf("[UpdateTrackedHeights] - %v", err)
		return err
	}

	newSections := buildSectionsFromSlice(heights)
	newHeights := buildSliceFromSections(newSections)

	newSections = append(newSections, dbSections...)
	mergedSections := mergeSections(newSections)

	sectionId = SectionId{Sections: mergedSections, IndexerId: id}

	// Write new sections to db
	err = updateDb(sectionId, db)
	if err != nil {
		zap.S().Errorf("[UpdateTrackedHeights] - %v", err)
		return err
	}

	if newHeights != nil {
		updateMissingHeights(id, len(*newHeights))
	}

	return nil
}

func UpdateInProgressHeight(track bool, heights *[]uint64, id string, db *gorm.DB) error {
	var err error
	if track {
		err = UpdateTrackedHeights(heights, id+WipStr, db)
	} else {
		err = RemoveHeights(heights, id+WipStr, db)
	}

	if err != nil {
		zap.S().Errorf(err.Error())
		return err
	}

	return nil
}

func ClearInProgress(id string, db *gorm.DB) error {
	updateMutex.Lock()
	defer updateMutex.Unlock()

	tx := db.Delete(DbSection{}, "indexer_id = ?", id+WipStr)
	if tx.Error != nil {
		zap.S().Errorf("[ClearInProgress]- %v", tx.Error.Error())
		return tx.Error
	}

	return nil
}

func GetMissingHeights(chainTip uint64, genesisHeight uint64, limit uint64, id string, db *gorm.DB) (*[]uint64, error) {
	updateMutex.Lock()
	defer updateMutex.Unlock()

	// Get current currentTracked stored on DB
	currentTracked, err := readDb(id, db)
	if err != nil {
		return nil, err
	}

	// Get WIP heights for this id
	currentInProgress, err := readDb(id+WipStr, db)
	if err != nil {
		return nil, err
	}

	dbSections := currentTracked.Sections
	dbSections = append(dbSections, currentInProgress.Sections...)

	dbSections = append(dbSections,
		Section{
			StartIdx: genesisHeight,
			EndIdx:   genesisHeight,
		},
		Section{
			StartIdx: chainTip,
			EndIdx:   chainTip,
		},
	)

	missing := findGapsInSections(dbSections)
	setTotalMissingHeightsMetric(id, len(*missing))

	if limit != NoReturnLimit && uint64(len(*missing)) > limit {
		l := *missing
		l = l[:limit]
		return &l, nil
	}

	return missing, nil
}

func RemoveHeights(heights *[]uint64, id string, db *gorm.DB) error {
	var sections Sections
	for _, h := range *heights {
		sections = append(sections, Section{
			StartIdx: h,
			EndIdx:   h,
		})
	}

	return RemoveSectionsFromTracker(sections, id, db)
}

func RemoveSectionsFromTracker(toRemove Sections, id string, db *gorm.DB) error {
	updateMutex.Lock()
	defer updateMutex.Unlock()

	currentSectionId, err := readDb(id, db)
	if err != nil {
		return err
	}

	remainingSections := RemoveSections(currentSectionId.Sections, toRemove)

	err = updateDb(SectionId{
		Sections:  remainingSections,
		IndexerId: id,
	}, db)
	if err != nil {
		zap.S().Errorf("[RemoveSectionsFromTracker] - %v", err)
		return err
	}

	return nil
}

func GetTrackedHeights(id string, db *gorm.DB) (*[]uint64, error) {
	updateMutex.Lock()
	defer updateMutex.Unlock()

	sectionId, err := readDb(id, db)
	if err != nil {
		return nil, err
	}

	tracked := buildSliceFromSections(sectionId.Sections)
	return tracked, nil
}

func GetTrackedTip(db *gorm.DB, refTrackId string) (uint64, error) {
	var tipHeight uint64
	tx := db.Model(&Section{}).Select("COALESCE(MAX(end_idx), 0)").Find(&tipHeight, "indexer_id = ?", refTrackId)

	return tipHeight, tx.Error
}

// Converts struct SectionID to struct DbSection
func convertSectionId(section SectionId) *DbSections {
	result := make(DbSections, 0, len(section.Sections))

	for _, sec := range section.Sections {
		result = append(result, DbSection{IndexerId: section.IndexerId, Section: Section{StartIdx: sec.StartIdx, EndIdx: sec.EndIdx}})
	}

	return &result
}

func readDb(id string, db *gorm.DB) (SectionId, error) {
	var dbSections Sections
	tx := db.Model(&DbSection{}).Find(&dbSections, "indexer_id = ?", id)

	result := SectionId{IndexerId: id, Sections: dbSections}

	return result, tx.Error
}

func updateDb(section SectionId, db *gorm.DB) error {
	err := db.Transaction(func(sqlTx *gorm.DB) error {
		if err := sqlTx.Delete(&DbSections{}, "indexer_id = ?", section.IndexerId).Error; err != nil {
			return err
		}

		if err := sqlTx.CreateInBatches(convertSectionId(section), 20000).Error; err != nil {
			return err
		}
		return nil
	})

	return err
}
