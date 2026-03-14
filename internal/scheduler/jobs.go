package scheduler

import (
	"encoding/json"
	"fmt"

	"github.com/savvients/sip-core/internal/jobs"
)

// JobTypes is the list of job types the scheduler can generate.
var JobTypes = []string{
	jobs.EmailType,
	jobs.ImageType,
	jobs.InvoiceType,
	jobs.ReportType,
}

// GenerateFake produces a fake payload and retry count for the given job type.
// Returns (payload JSON, maxRetry, error). maxRetry is 2 for all fake jobs.
func GenerateFake(jobType string) (payload []byte, maxRetry int32, err error) {
	maxRetry = 2
	switch jobType {
	case jobs.EmailType:
		payload, err = json.Marshal(GenerateEmailPayload())
	case jobs.ImageType:
		payload, err = json.Marshal(GenerateImagePayload())
	case jobs.InvoiceType:
		payload, err = json.Marshal(GenerateInvoicePayload())
	case jobs.ReportType:
		payload, err = json.Marshal(GenerateReportPayload())
	default:
		return nil, 0, fmt.Errorf("unknown job type: %s", jobType)
	}
	return payload, maxRetry, err
}
