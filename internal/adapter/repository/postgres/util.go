package postgres

import (
	"errors"
	"strconv"

	"github.com/jackc/pgx/v5"
)

func itoa(i int) string { return strconv.Itoa(i) }

func isNoRows(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
