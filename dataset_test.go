package ddataset

import (
	"github.com/lawrencewoodman/dlit"
	"testing"
)

func TestRecordClone(t *testing.T) {
	record := make(Record, 2)
	getRecord := func(row int) Record {
		if row == 0 {
			record["name"] = dlit.NewString("Mary Williams")
			record["age"] = dlit.MustNew(27)
		} else {
			record["name"] = dlit.NewString("Dewi Thomas")
			record["age"] = dlit.MustNew(29)
		}
		return record
	}
	records := []Record{getRecord(0).Clone(), getRecord(1).Clone()}

	if records[0]["name"].String() != "Mary Williams" {
		t.Errorf("records[0][\"name\"]: %s, want: Mary Williams", records[0]["name"])
	}
	if records[0]["age"].String() != "27" {
		t.Errorf("records[0][\"age\"]: %s, want: 27", records[0]["age"])
	}
	if records[1]["name"].String() != "Dewi Thomas" {
		t.Errorf("records[1][\"name\"]: %s, want: Dewi Thomas", records[1]["name"])
	}
	if records[1]["age"].String() != "29" {
		t.Errorf("records[1][\"age\"]: %s, want: 29", records[1]["age"])
	}
}
