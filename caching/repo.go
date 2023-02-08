package caching

import (
	"context"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func NewDB() *sqlx.DB {
	return sqlx.MustConnect("mysql", "root:1@tcp(localhost:3306)/bench?parseTime=true")
}

type Repository struct {
	db *sqlx.DB
}

func NewRepository(db *sqlx.DB) *Repository {
	return &Repository{
		db: db,
	}
}

func (r *Repository) InsertProducts(ctx context.Context, products []ProductContent) error {
	query := `
INSERT INTO products (sku, content_data)
VALUES (:sku, :content_data)
`
	_, err := r.db.NamedExecContext(ctx, query, products)
	return err
}

func (r *Repository) GetProducts(ctx context.Context, skus []string) ([]ProductContent, error) {
	query, args, err := sqlx.In(`SELECT sku, content_data FROM products WHERE sku IN (?)`, skus)
	if err != nil {
		return nil, err
	}

	var result []ProductContent
	err = r.db.SelectContext(ctx, &result, query, args...)
	return result, err
}
