package scanner

import (
	"fmt"
	"time"

	"gorm.io/gorm"
)

type ScannerTask struct {
	ID        uint      `gorm:"primarykey;autoIncrement"`
	CreatedAt time.Time `gorm:"type:timestamp(5)"`
	UpdatedAt time.Time `gorm:"type:timestamp(5)"`

	Name      string `gorm:"type:varchar(256);unique;not null"`
	NextBlock uint64 `gorm:"type:numeric(20,0)"`
	JobToken  string `gorm:"type:string"`
}

func (ScannerTask) TableName() string {
	return "scanner_tasks"
}

// GetScannerTaskByName returns a task with given name
func GetScannerTaskByName(db *gorm.DB, taskName string) (*ScannerTask, error) {
	model := &ScannerTask{}

	result := db.
		Where("name = ?", taskName).
		Take(model)

	if result.Error == gorm.ErrRecordNotFound {
		return nil, nil
	} else if result.Error != nil {
		return nil, result.Error
	}

	return model, nil
}

// CreateScannerTaskByName inserts a new task into the database.
func CreateScannerTaskByName(db *gorm.DB, taskName string, nextBlock uint64) (*ScannerTask, error) {
	now := time.Now()
	model := &ScannerTask{
		Name:      taskName,
		NextBlock: nextBlock,
		CreatedAt: now,
		UpdatedAt: now,
	}

	result := db.Create(model)
	if result.Error != nil {
		return nil, result.Error
	}

	return model, nil
}

// UpdateNextBlock updates the next block to proceed with atomicity ensured.
func UpdateNextBlock(db *gorm.DB, m *ScannerTask, nextBlock uint64, newJobToken string) error {
	updatedAt := time.Now()

	result := db.
		Model(&ScannerTask{}).
		Where("id = ?", m.ID).
		Where("name = ?", m.Name).
		Where("next_block = ?", m.NextBlock).
		Where("job_token = ?", m.JobToken).
		UpdateColumns(&ScannerTask{
			UpdatedAt: updatedAt,
			NextBlock: nextBlock,
			JobToken:  newJobToken,
		})

	if result.Error != nil {
		return result.Error
	} else if result.RowsAffected != 1 {
		return fmt.Errorf("rowAffected(%v) is not equal to 1", result.RowsAffected)
	}

	m.UpdatedAt = updatedAt
	m.NextBlock = nextBlock
	m.JobToken = newJobToken

	return nil
}

// RemoveTask removes the task from the database.
func RemoveTask(db *gorm.DB, taskName string) error {
	result := db.Where("name = ?", taskName).Delete(&ScannerTask{})

	if result.Error != nil {
		return result.Error
	}

	return nil
}
