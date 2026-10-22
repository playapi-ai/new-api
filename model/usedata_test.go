package model

import (
	"os"
	"testing"

	"github.com/glebarez/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func TestDashboardCacheUsageDatabaseMatrix(t *testing.T) {
	for _, dialect := range []string{"sqlite", "mysql", "postgres"} {
		t.Run(dialect, func(t *testing.T) {
			var driver gorm.Dialector = sqlite.Open(":memory:")
			if dialect == "mysql" {
				dsn := os.Getenv("TEST_MYSQL_DSN")
				if dsn == "" {
					t.Skip("TEST_MYSQL_DSN is not configured")
				}
				driver = mysql.Open(dsn)
			} else if dialect == "postgres" {
				dsn := os.Getenv("TEST_POSTGRES_DSN")
				if dsn == "" {
					t.Skip("TEST_POSTGRES_DSN is not configured")
				}
				driver = postgres.Open(dsn)
			}
			db, err := gorm.Open(driver, &gorm.Config{})
			require.NoError(t, err)
			sqlDB, err := db.DB()
			require.NoError(t, err)
			sqlDB.SetMaxOpenConns(1)
			previousDB, previousCache := DB, CacheQuotaData
			DB, CacheQuotaData = db, make(map[string]*QuotaData)
			t.Cleanup(func() {
				DB, CacheQuotaData = previousDB, previousCache
				require.NoError(t, db.Migrator().DropTable(&QuotaData{}))
				require.NoError(t, sqlDB.Close())
			})
			for _, upgrade := range []bool{false, true} {
				t.Run(map[bool]string{false: "fresh", true: "existing_fork"}[upgrade], func(t *testing.T) {
					require.NoError(t, db.Migrator().DropTable(&QuotaData{}))
					if upgrade {
						require.NoError(t, db.AutoMigrate(&QuotaData{}))
						require.NoError(t, db.Migrator().DropColumn(&QuotaData{}, "cache_read_tokens"))
						require.NoError(t, db.Migrator().DropColumn(&QuotaData{}, "cache_creation_tokens"))
						require.NoError(t, db.Table("quota_data").Create(map[string]any{
							"user_id": 42, "username": "dashboard-probe", "model_name": "model-probe", "created_at": 3600,
							"count": 1, "quota": 10, "token_used": 100,
						}).Error)
					}
					for range 2 {
						require.NoError(t, db.AutoMigrate(&QuotaData{}))
					}
					if upgrade {
						var previous QuotaData
						require.NoError(t, db.First(&previous).Error)
						assert.Equal(t, 100, previous.TokenUsed)
						assert.Zero(t, previous.CacheReadTokens)
						assert.Zero(t, previous.CacheCreationTokens)
					}
					for range 2 {
						LogQuotaData(QuotaDataLogParams{UserID: 42, Username: "dashboard-probe", ModelName: "model-probe",
							CreatedAt: 3601, TokenUsed: 200, CacheReadTokens: 30, CacheCreationTokens: 20, Quota: 5})
					}
					SaveQuotaDataCache()
					LogQuotaData(QuotaDataLogParams{UserID: 42, Username: "dashboard-probe", ModelName: "model-probe",
						CreatedAt: 3602, TokenUsed: 50, CacheReadTokens: -5, CacheCreationTokens: -2, Quota: 1})
					SaveQuotaDataCache()
					queries := []struct {
						name string
						read func() ([]*QuotaData, error)
					}{
						{"username", func() ([]*QuotaData, error) { return GetQuotaDataByUsername("dashboard-probe", 3600, 3600) }},
						{"user_id", func() ([]*QuotaData, error) { return GetQuotaDataByUserId(42, 3600, 3600) }},
						{"grouped_users", func() ([]*QuotaData, error) { return GetQuotaDataGroupByUser(3600, 3600) }},
						{"all_models", func() ([]*QuotaData, error) { return GetAllQuotaDates(3600, 3600, "") }},
					}
					for _, query := range queries {
						t.Run(query.name, func(t *testing.T) {
							rows, err := query.read()
							require.NoError(t, err)
							require.Len(t, rows, 1)
							wantTokens, wantCount, wantQuota := 450, 3, 11
							if upgrade {
								wantTokens, wantCount, wantQuota = 550, 4, 21
							}
							assert.Equal(t, wantTokens, rows[0].TokenUsed)
							assert.Equal(t, wantCount, rows[0].Count)
							assert.Equal(t, wantQuota, rows[0].Quota)
							assert.Equal(t, int64(60), rows[0].CacheReadTokens)
							assert.Equal(t, int64(40), rows[0].CacheCreationTokens)
						})
					}
				})
			}
		})
	}
}
