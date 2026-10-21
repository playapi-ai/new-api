package model

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/bytedance/gopkg/util/gopool"
	"github.com/google/uuid"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// BillingOperation is both a durable retry intent and an exactly-once receipt.
// Quotas are signed debits. No credentials or usable API keys are persisted.
type BillingOperation struct {
	ID         string `gorm:"type:varchar(36);primaryKey"`
	Kind       string `gorm:"type:varchar(24);not null"`
	AccountID  int
	Quota      int
	TokenID    int
	TokenQuota int
	RequestID  string `gorm:"type:varchar(64)"`
	DependsOn  string `gorm:"type:varchar(36)"`
	ReverseID  string `gorm:"type:varchar(36)"`
	State      int    `gorm:"not null;index:idx_billing_operation_pending"` // 0 pending, 1 applied
	CacheState int    `gorm:"not null;default:0;index:idx_billing_operation_cache"`
	UserID     int
	CreatedAt  int64
}

var billingRecoveryMemory sync.Map
var billingRecoveryOnce sync.Once

// ApplyBillingOperation retains the intent before changing either balance.
// If the database cannot even store the intent, the worker retains it in memory.
func ApplyBillingOperation(op BillingOperation) error {
	if _, err := uuid.Parse(op.ID); err != nil {
		return err
	}
	if op.DependsOn == op.ID || op.ReverseID == op.ID || op.State != 0 || op.Quota < -common.MaxQuota || op.Quota > common.MaxQuota || op.TokenQuota < -common.MaxQuota || op.TokenQuota > common.MaxQuota {
		return errors.New("invalid billing operation quota")
	}
	op.CreatedAt = common.GetTimestamp()
	billingRecoveryMemory.Store(op.ID, op)
	if err := DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&op).Error; err != nil {
		return err
	}
	billingRecoveryMemory.Delete(op.ID)
	return runBillingOperation(op.ID)
}

func runBillingOperation(id string) error {
	// Resolve dependencies before opening a transaction, including on SQLite
	// with a single connection. The dependent receipt is checked again below.
	var pending BillingOperation
	if err := DB.Where("id = ?", id).First(&pending).Error; err != nil {
		return err
	}
	if pending.State == 0 && pending.DependsOn != "" {
		if err := runBillingOperation(pending.DependsOn); err != nil {
			return err
		}
	}
	if pending.State == 0 && pending.ReverseID != "" {
		var original BillingOperation
		found := DB.Where("id = ?", pending.ReverseID).Limit(1).Find(&original)
		if found.Error != nil {
			return found.Error
		}
		if found.RowsAffected > 0 {
			if err := runBillingOperation(original.ID); err != nil {
				return err
			}
		}
	}
	var applied BillingOperation
	err := DB.Transaction(func(tx *gorm.DB) error {
		var op BillingOperation
		if err := lockForUpdate(tx).Where("id = ?", id).First(&op).Error; err != nil {
			return err
		}
		if op.State == 1 {
			applied = op
			return nil
		}
		if op.DependsOn != "" {
			var dependency BillingOperation
			if err := tx.Where("id = ?", op.DependsOn).First(&dependency).Error; err != nil {
				return err
			}
			if dependency.State != 1 {
				return errors.New("billing compensation is still pending")
			}
		}
		if op.ReverseID != "" {
			var reservation BillingOperation
			result := tx.Where("id = ?", op.ReverseID).Limit(1).Find(&reservation)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				return tx.Model(&op).Updates(map[string]any{"state": 1, "cache_state": 1}).Error
			}
			op.Kind, op.AccountID, op.Quota = reservation.Kind, reservation.AccountID, -reservation.Quota
			op.TokenID, op.TokenQuota = reservation.TokenID, -reservation.TokenQuota
		}
		switch op.Kind {
		case "wallet":
			query := tx.Model(&User{}).Where("id = ?", op.AccountID)
			if op.Quota < 0 {
				query = query.Where("quota <= ?", common.MaxWalletQuota+op.Quota)
			}
			result := query.Update("quota", gorm.Expr("quota - ?", op.Quota))
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 && op.Quota != 0 {
				return errors.New("billing wallet unavailable or quota limit exceeded")
			}
		case "subscription":
			if err := postConsumeUserSubscriptionDeltaTx(tx, op.AccountID, int64(op.Quota)); err != nil {
				return err
			}
		case "subscription_refund":
			if err := refundSubscriptionPreConsumeTx(tx, op.RequestID); err != nil {
				return err
			}
		case "token":
		default:
			return fmt.Errorf("unsupported billing operation: %s", op.Kind)
		}
		if op.TokenID > 0 && op.TokenQuota != 0 {
			result := tx.Model(&Token{}).Where("id = ?", op.TokenID).Updates(map[string]any{
				"remain_quota":  gorm.Expr("remain_quota - ?", op.TokenQuota),
				"used_quota":    gorm.Expr("used_quota + ?", op.TokenQuota),
				"accessed_time": common.GetTimestamp(),
			})
			if result.Error != nil {
				return result.Error
			}
			// A token may have been deleted after the request was admitted. Its
			// absence must not prevent the funding-side settlement/refund.
		}
		if err := tx.Model(&op).Updates(map[string]any{"state": 1, "cache_state": 0, "kind": op.Kind, "account_id": op.AccountID, "quota": op.Quota, "token_id": op.TokenID, "token_quota": op.TokenQuota}).Error; err != nil {
			return err
		}
		applied = op
		return nil
	})
	if err != nil {
		return err
	}
	if applied.ID == "" {
		return nil
	}
	if applied.CacheState == 1 {
		return nil
	}
	if common.RedisEnabled {
		return syncBillingOperationCache(applied)
	}
	return DB.Model(&BillingOperation{}).Where("id = ?", applied.ID).Update("cache_state", 1).Error
}

// InitBillingRecovery starts independently of optional batch accounting.
func InitBillingRecovery() {
	billingRecoveryOnce.Do(func() {
		gopool.Go(func() {
			ticker := time.NewTicker(5 * time.Second)
			defer ticker.Stop()
			for {
				if err := RecoverBillingOperations(); err != nil {
					common.SysError(common.LogText("billing recovery: %s", err.Error()))
				}
				<-ticker.C
			}
		})
	})
}

// RecoverBillingOperations resumes persisted operations without their sessions.
func RecoverBillingOperations() error {
	billingRecoveryMemory.Range(func(_, value any) bool {
		op := value.(BillingOperation)
		if err := ApplyBillingOperation(op); err != nil {
			common.SysError(common.LogText("billing recovery: %s", err.Error()))
		}
		return true
	})
	var pending []BillingOperation
	if err := DB.Where("state = ? OR cache_state = ?", 0, 0).Order("created_at, id").Limit(200).Find(&pending).Error; err != nil {
		return err
	}
	var result error
	for _, op := range pending {
		if err := runBillingOperation(op.ID); err != nil {
			result = errors.Join(result, fmt.Errorf("billing operation %s: %w", op.ID, err))
		}
	}
	return result
}

// ReserveBillingFunding records the reservation and its receipt atomically.
// A rejected reservation is never automatically replayed.
func ReserveBillingFunding(op BillingOperation, requireFunds bool) error {
	if _, err := uuid.Parse(op.ID); err != nil {
		return err
	}
	if op.Quota <= 0 || op.Quota > common.MaxQuota {
		return errors.New("invalid billing reservation")
	}
	cacheReserved := false
	var tokenKey string
	if op.Kind == "token" {
		var token Token
		result := DB.Select("id", "key").Where("id = ?", op.TokenID).Limit(1).Find(&token)
		if result.Error != nil {
			return result.Error
		}
		tokenKey = token.Key
	}
	if common.RedisEnabled && (op.Kind == "wallet" && requireFunds || op.Kind == "token") {
		result, err := cacheReserveBillingQuota(op, tokenKey, requireFunds)
		if err == nil && result == cacheQuotaMiss {
			if op.Kind == "wallet" {
				if _, hydrateErr := GetUserCache(op.AccountID); hydrateErr == nil {
					result, err = cacheReserveBillingQuota(op, tokenKey, requireFunds)
				}
			} else if _, hydrateErr := GetTokenByKey(tokenKey, true); hydrateErr == nil {
				result, err = cacheReserveBillingQuota(op, tokenKey, requireFunds)
			}
		}
		if err == nil && result == cacheQuotaInsufficient {
			return ErrBillingInsufficientQuota
		}
		cacheReserved = err == nil && result == cacheQuotaOK
	}
	err := DB.Transaction(func(tx *gorm.DB) error {
		switch op.Kind {
		case "wallet":
			query := tx.Model(&User{}).Where("id = ?", op.AccountID)
			if requireFunds && !cacheReserved {
				query = query.Where("quota >= ?", op.Quota)
			}
			result := query.Update("quota", gorm.Expr("quota - ?", op.Quota))
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrBillingInsufficientQuota
			}
		case "token":
			query := tx.Model(&Token{}).Where("id = ?", op.TokenID)
			if requireFunds && !cacheReserved {
				query = query.Where("remain_quota >= ?", op.Quota)
			}
			result := query.Updates(map[string]any{"remain_quota": gorm.Expr("remain_quota - ?", op.Quota), "used_quota": gorm.Expr("used_quota + ?", op.Quota), "accessed_time": common.GetTimestamp()})
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrBillingInsufficientQuota
			}
		case "subscription":
			if err := postConsumeUserSubscriptionDeltaTx(tx, op.AccountID, int64(op.Quota)); err != nil {
				return err
			}
		default:
			return errors.New("unsupported billing reservation")
		}
		op.State, op.CreatedAt = 1, common.GetTimestamp()
		return tx.Create(&op).Error
	})
	if err != nil && cacheReserved {
		var receipt BillingOperation
		confirmation := DB.Where("id = ?", op.ID).Limit(1).Find(&receipt)
		if confirmation.Error == nil && confirmation.RowsAffected > 0 {
			return runBillingOperation(op.ID)
		}
		if confirmation.Error == nil {
			_ = common.RDB.Expire(context.Background(), "billing:cache:"+op.ID+":"+op.Kind, 24*time.Hour).Err()
			if op.Kind == "wallet" {
				if _, cacheErr := cacheApplyUserQuotaDelta(op.AccountID, int64(op.Quota)); cacheErr != nil {
					_ = invalidateUserCache(op.AccountID)
				}
			} else if _, cacheErr := cacheApplyTokenQuotaDelta(op.TokenID, tokenKey, int64(op.Quota)); cacheErr != nil {
				_ = invalidateTokenCacheForMutation(tokenKey)
			}
		}
	}
	if err == nil {
		return runBillingOperation(op.ID)
	}
	return err
}

var ErrBillingInsufficientQuota = errors.New("wallet quota insufficient")

// RecoverBillingAccount finishes pending debits before admitting another
// request against a balance that has not yet reflected the earlier charge.
func RecoverBillingAccount(userID, tokenID int) error {
	var memoryErr error
	billingRecoveryMemory.Range(func(_, value any) bool {
		op := value.(BillingOperation)
		if op.UserID == userID || op.Kind == "wallet" && op.AccountID == userID || op.TokenID == tokenID && tokenID > 0 {
			if err := ApplyBillingOperation(op); err != nil {
				memoryErr = errors.Join(memoryErr, err)
			}
		}
		return true
	})
	if memoryErr != nil {
		return memoryErr
	}
	var pending []BillingOperation
	if err := DB.Where("(state = ? OR cache_state = ?) AND ((user_id = ? AND quota > 0) OR (kind = ? AND account_id = ? AND quota > 0) OR (token_id = ? AND token_quota > 0))", 0, 0, userID, "wallet", userID, tokenID).Find(&pending).Error; err != nil {
		return err
	}
	for _, op := range pending {
		if err := runBillingOperation(op.ID); err != nil {
			return err
		}
	}
	return nil
}

// The Redis receipt survives a lost SQL or Redis acknowledgement. It expires
// only after SQL acknowledges cache delivery, so the worker can safely retry.
func syncBillingOperationCache(op BillingOperation) error {
	if !common.RedisEnabled {
		return nil
	}
	ctx := context.Background()
	const script = `
if redis.call('EXISTS',KEYS[2]) == 1 then return 1 end
if tonumber(redis.call('HGET',KEYS[1],'Id') or '0') == tonumber(ARGV[1]) then
 if ARGV[2] == 'wallet' and redis.call('HEXISTS',KEYS[1],'Quota') == 1 then
  redis.call('HINCRBY',KEYS[1],'Quota',-tonumber(ARGV[3]))
 elseif ARGV[2] == 'token' and redis.call('HEXISTS',KEYS[1],'RemainQuota') == 1 and redis.call('HEXISTS',KEYS[1],'UsedQuota') == 1 then
  redis.call('HINCRBY',KEYS[1],'RemainQuota',-tonumber(ARGV[3]))
  redis.call('HINCRBY',KEYS[1],'UsedQuota',tonumber(ARGV[3]))
 end
end
redis.call('SET',KEYS[2],1)
return 1`
	var receipts []string
	if op.Kind == "wallet" && op.Quota != 0 {
		receipt := "billing:cache:" + op.ID + ":wallet"
		if err := common.RDB.Eval(ctx, script, []string{getUserCacheKey(op.AccountID), receipt}, op.AccountID, "wallet", op.Quota).Err(); err != nil {
			return err
		}
		receipts = append(receipts, receipt)
	}
	if op.TokenID > 0 && op.TokenQuota != 0 {
		var token Token
		result := DB.Select("id", "key").Where("id = ?", op.TokenID).Limit(1).Find(&token)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected > 0 {
			receipt := "billing:cache:" + op.ID + ":token"
			if err := common.RDB.Eval(ctx, script, []string{getTokenCacheKey(token.Key), receipt}, op.TokenID, "token", op.TokenQuota).Err(); err != nil {
				return err
			}
			receipts = append(receipts, receipt)
		}
	}
	if err := DB.Model(&BillingOperation{}).Where("id = ?", op.ID).Update("cache_state", 1).Error; err != nil {
		return err
	}
	for _, receipt := range receipts {
		if err := common.RDB.Expire(ctx, receipt, 24*time.Hour).Err(); err != nil {
			common.SysError(common.LogText("billing cache receipt expiry: %s", err.Error()))
		}
	}
	return nil
}

func cacheReserveBillingQuota(op BillingOperation, tokenKey string, requireFunds bool) (cacheQuotaResult, error) {
	const script = `
if redis.call('EXISTS',KEYS[2]) == 1 then return 1 end
if tonumber(redis.call('HGET',KEYS[1],'Id') or '0') ~= tonumber(ARGV[2]) then return -1 end
local balance='Quota'
if ARGV[4]=='wallet' then
 if tonumber(redis.call('HGET',KEYS[1],'CacheSchema') or '0') ~= tonumber(ARGV[3]) then return -1 end
else
 balance='RemainQuota'
 if redis.call('HEXISTS',KEYS[1],'UsedQuota') == 0 then return -1 end
end
if redis.call('HEXISTS',KEYS[1],balance) == 0 then return -1 end
if ARGV[5]=='true' and tonumber(redis.call('HGET',KEYS[1],balance)) < tonumber(ARGV[1]) then return 0 end
redis.call('HINCRBY',KEYS[1],balance,-tonumber(ARGV[1]))
if ARGV[4]=='token' then redis.call('HINCRBY',KEYS[1],'UsedQuota',tonumber(ARGV[1])) end
redis.call('SET',KEYS[2],1)
return 1`
	cacheKey := getUserCacheKey(op.AccountID)
	accountID := op.AccountID
	if op.Kind == "token" {
		cacheKey = getTokenCacheKey(tokenKey)
		accountID = op.TokenID
	}
	result, err := common.RDB.Eval(context.Background(), script, []string{cacheKey, "billing:cache:" + op.ID + ":" + op.Kind}, op.Quota, accountID, userCacheSchemaVersion, op.Kind, strconv.FormatBool(requireFunds)).Int()
	return quotaResultFromLua(result, err)
}

// ReserveBillingTokenQuota persists admission before a subsequent settlement
// or refund can commit, including when legacy batch accounting is enabled.
func ReserveBillingTokenQuota(id int, quota int, unlimited bool) (bool, error) {
	if quota < 0 {
		return false, errors.New("quota cannot be negative")
	}
	if quota == 0 {
		return true, nil
	}
	op := BillingOperation{ID: uuid.NewString(), Kind: "token", AccountID: id, Quota: quota, TokenID: id, TokenQuota: quota}
	if err := ReserveBillingFunding(op, !unlimited); err != nil {
		if errors.Is(err, ErrBillingInsufficientQuota) {
			return false, nil
		}
		if refundErr := ApplyBillingOperation(BillingOperation{ID: uuid.NewString(), Kind: "token", ReverseID: op.ID}); refundErr != nil {
			common.SysError(common.LogText("error retaining rejected token reservation: %s", refundErr.Error()))
		}
		return false, err
	}
	return true, nil
}
