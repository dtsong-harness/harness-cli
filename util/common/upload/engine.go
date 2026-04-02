package upload

import (
	"context"
	"fmt"
	"sync"
	"time"

	p "github.com/harness/harness-cli/util/common/progress"
)

// manages concurrent file uploads
type FileUploadEngine struct {
	maxWorkers int
	progress   p.Reporter
}

// createing a new upload engine , to perform upload concurrently
func NewFileUploadEngine(maxWorkers int, progress p.Reporter) *FileUploadEngine {
	if maxWorkers <= 0 {
		maxWorkers = 5
	}
	return &FileUploadEngine{
		maxWorkers: maxWorkers,
		progress:   progress,
	}
}

// Execute runs all upload jobs concurrently
func (e *FileUploadEngine) Execute(ctx context.Context, jobs []FileUploadJob) []FileUploadResult {
	if len(jobs) == 0 {
		return nil
	}

	numWorkers := e.maxWorkers
	if len(jobs) < numWorkers {
		numWorkers = len(jobs)
	}

	e.progress.Step(fmt.Sprintf("Starting upload: %d files with %d workers. Please wait ....", len(jobs), numWorkers))

	startTime := time.Now()
	jobChan := make(chan FileUploadJob, len(jobs))
	resultChan := make(chan FileUploadResult, len(jobs))

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Start workers
	for i := 0; i < numWorkers; i++ {
		go e.worker(ctx, &wg, jobChan, resultChan)
	}

	// Send jobs to workers
	for _, job := range jobs {
		jobChan <- job
	}
	close(jobChan)

	// Wait for all workers to finish
	wg.Wait()
	close(resultChan)

	// Collect results
	results := make([]FileUploadResult, 0, len(jobs))
	successCount := 0
	for result := range resultChan {
		results = append(results, result)
		if result.Success {
			successCount++
		}
	}

	duration := time.Since(startTime)

	// Report summary
	if successCount == len(jobs) {
		e.progress.Success(fmt.Sprintf("Successfully uploaded %d files in %v (%.2f files/sec)",
			len(jobs), duration, float64(len(jobs))/duration.Seconds()))
	} else {
		failCount := len(jobs) - successCount
		e.progress.Error(fmt.Sprintf("Upload completed with errors: %d/%d succeeded, %d failed in %v",
			successCount, len(jobs), failCount, duration))
	}

	return results
}

// worker processes upload jobs from the job channel
func (e *FileUploadEngine) worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan FileUploadJob, results chan<- FileUploadResult) {
	defer wg.Done()

	for job := range jobs {
		select {
		case <-ctx.Done():
			results <- FileUploadResult{
				JobID:    job.GetID(),
				FilePath: job.GetFilePath(),
				FileSize: job.GetFileSize(),
				Error:    ctx.Err(),
				Success:  false,
			}
			return
		default:
			err := job.Upload(ctx)
			results <- FileUploadResult{
				JobID:    job.GetID(),
				FilePath: job.GetFilePath(),
				FileSize: job.GetFileSize(),
				Error:    err,
				Success:  err == nil,
			}
		}
	}
}

// checks if any results contain errors
func HasUploadErrors(results []FileUploadResult) bool {
	for _, result := range results {
		if result.Error != nil {
			return true
		}
	}
	return false
}

// this will  returns a map of failed uploads
func GetUploadErrors(results []FileUploadResult) map[string]error {
	errors := make(map[string]error)
	for _, result := range results {
		if result.Error != nil {
			errors[result.JobID] = result.Error
		}
	}
	return errors
}

// returns count of successful uploads
func GetSuccessfulUploads(results []FileUploadResult) int {
	count := 0
	for _, result := range results {
		if result.Success {
			count++
		}
	}
	return count
}
