package scheduler

import (
	"math/rand"
	"strconv"

	"github.com/savvients/sip-core/internal/jobs"
)

var (
	emailRecipients = []string{"user@example.com", "billing@test.com", "support@demo.org", "noreply@localhost"}
	emailSubjects   = []string{"Welcome", "Invoice ready", "Report generated", "Reminder", "Your order"}
	emailBodies     = []string{"Please find attached.", "Thank you for your business.", "Generated at request."}

	imageURLs = []string{"https://picsum.photos/400/300", "https://via.placeholder.com/800/600"}
	imagePaths = []string{"/tmp/sample.png", ""}

	invoiceTemplates = []string{"Invoice #{{.ID}}\nTotal: ${{.Total}}\nCustomer: {{.Customer}}", "Order {{.ID}} total {{.Total}}"}
	reportHeaders    = []string{"ID", "Name", "Amount", "Date"}
)

func init() {
	// Seed is set from default source; for tests, call rand.Seed explicitly.
}

// GenerateEmailPayload returns a random email payload.
func GenerateEmailPayload() jobs.EmailPayload {
	to := emailRecipients[rand.Intn(len(emailRecipients))]
	subj := emailSubjects[rand.Intn(len(emailSubjects))]
	body := emailBodies[rand.Intn(len(emailBodies))]
	return jobs.EmailPayload{To: to, Subject: subj, Body: body}
}

// GenerateImagePayload returns a random image resize payload (URL or path, dimensions).
func GenerateImagePayload() jobs.ImagePayload {
	useURL := rand.Intn(2) == 0
	width := uint(100 + rand.Intn(401))   // 100–500
	height := uint(80 + rand.Intn(321))   // 80–400
	sourceURL := ""
	sourcePath := ""
	if useURL {
		sourceURL = imageURLs[rand.Intn(len(imageURLs))]
	} else {
		sourcePath = imagePaths[rand.Intn(len(imagePaths))]
	}
	return jobs.ImagePayload{
		SourceURL:  sourceURL,
		SourcePath: sourcePath,
		Width:      width,
		Height:     height,
		OutPath:    "",
	}
}

// GenerateInvoicePayload returns a random invoice payload (template + data).
func GenerateInvoicePayload() jobs.InvoicePayload {
	tpl := invoiceTemplates[rand.Intn(len(invoiceTemplates))]
	id := strconv.Itoa(1000 + rand.Intn(9000))
	total := strconv.FormatFloat(10.0+rand.Float64()*990.0, 'f', 2, 64)
	customer := []string{"Acme Inc", "Demo User", "Test Co"}[rand.Intn(3)]
	return jobs.InvoicePayload{
		Template: tpl,
		Data: map[string]any{
			"ID":       id,
			"Total":    total,
			"Customer": customer,
		},
		OutPath: "",
	}
}

// GenerateReportPayload returns a random report payload (headers + rows).
func GenerateReportPayload() jobs.ReportPayload {
	rows := [][]string{
		{"1", "Item A", "10.00", "2025-03-14"},
		{"2", "Item B", "25.50", "2025-03-14"},
		{"3", "Item C", "8.99", "2025-03-14"},
	}
	if n := rand.Intn(3); n > 0 {
		rows = rows[:1+n]
	}
	return jobs.ReportPayload{
		Headers: reportHeaders,
		Rows:    rows,
		OutPath: "",
	}
}
