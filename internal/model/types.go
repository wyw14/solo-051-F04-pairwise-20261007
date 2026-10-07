package model

import "time"

type RunState string

const (
	Loading      RunState = "loading"
	Pumping      RunState = "pumping"
	Conditioning RunState = "conditioning"
	Coating      RunState = "coating"
	Cooling      RunState = "cooling"
	Venting      RunState = "venting"
	Released     RunState = "released"
	Failed       RunState = "failed"
)

type ID string

type Run struct {
	ID        ID        `json:"id"`
	Batch     string    `json:"batch"`
	RecipeID  ID        `json:"recipe_id"`
	State     RunState  `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Layer     int       `json:"layer"`
	Error     string    `json:"error,omitempty"`
}

type Layer struct {
	Number           int     `json:"number"`
	Material         string  `json:"material"`
	TargetNanometers float64 `json:"target_nm"`
	RateNanometers   float64 `json:"rate_nm_s"`
}

type Recipe struct {
	ID        ID            `json:"id"`
	Name      string        `json:"name"`
	Revision  int           `json:"revision"`
	Layers    map[int]Layer `json:"layers"`
	Published bool          `json:"published"`
}

type ChamberSnapshot struct {
	Name        string    `json:"name"`
	Pressure    float64   `json:"pressure"`
	Stable      bool      `json:"stable"`
	Shutter     string    `json:"shutter"`
	SourcePower float64   `json:"source_power"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ThicknessSample struct {
	RunID        ID        `json:"run_id"`
	Seq          int64     `json:"seq"`
	Frequency    float64   `json:"frequency"`
	Transmission float64   `json:"transmission"`
	Nanometers   float64   `json:"nanometers"`
	Valid        bool      `json:"valid"`
	At           time.Time `json:"at"`
}

type Incident struct {
	ID       ID        `json:"id"`
	RunID    ID        `json:"run_id"`
	Severity string    `json:"severity"`
	Message  string    `json:"message"`
	At       time.Time `json:"at"`
}
