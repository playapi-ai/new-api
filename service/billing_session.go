package service

import (
	"errors"
	"fmt"
	"math"
	"net/http"
	"strings"
	"sync"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/logger"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/operation_setting"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// ---------------------------------------------------------------------------
// BillingSession — 统一计费会话
// ---------------------------------------------------------------------------

// BillingSession 封装单次请求的预扣费/结算/退款生命周期。
// 实现 relaycommon.BillingSettler 接口。
type BillingSession struct {
	relayInfo         *relaycommon.RelayInfo
	funding           FundingSource
	preConsumedQuota  int    // 实际预扣额度（信任用户可能为 0）
	tokenConsumed     int    // 令牌额度实际扣减量
	extraReserved     int    // 发送前补充预扣的额度（订阅退款时需要单独回滚）
	trusted           bool   // 是否命中信任额度旁路
	settled           bool   // Settle 全部完成（资金 + 令牌）
	refunded          bool   // Refund 已调用
	lang              string // 请求声明的语言，为空表示未声明
	settlementID      string
	settlementQuota   int
	refundID          string
	tokenRefundID     string
	extraRefundID     string
	reserveRollbackID string
	mu                sync.Mutex
}

// Settle 根据实际消耗额度进行结算。
// 资金、令牌和操作凭证在同一事务中提交，失败后由持久化任务恢复。
func (s *BillingSession) Settle(actualQuota int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.refunded {
		return errors.New("billing session already refunded")
	}
	if actualQuota < 0 || actualQuota > common.MaxQuota {
		return errors.New("invalid billing quota")
	}
	if s.settlementID != "" && actualQuota != s.settlementQuota {
		return errors.New("billing settlement amount already committed")
	}
	if s.settled {
		return nil
	}
	if s.settlementID == "" {
		s.settlementID = uuid.NewString()
		s.settlementQuota = actualQuota
	}
	delta := actualQuota - s.preConsumedQuota
	op := model.BillingOperation{ID: s.settlementID, UserID: s.relayInfo.UserId, Quota: delta, DependsOn: s.reserveRollbackID}
	switch funding := s.funding.(type) {
	case *WalletFunding:
		op.Kind, op.AccountID = "wallet", funding.userId
	case *SubscriptionFunding:
		op.Kind, op.AccountID = "subscription", funding.subscriptionId
	default:
		return errors.New("unsupported billing source")
	}
	if !s.relayInfo.IsPlayground {
		op.TokenID, op.TokenQuota = s.relayInfo.TokenId, delta
	}
	// Both balance changes and the receipt commit atomically. A retry never
	// needs the request-local session, and cannot repeat only one leg.
	if err := model.ApplyBillingOperation(op); err != nil {
		return err
	}
	s.settled = true
	if s.funding.Source() == BillingSourceSubscription {
		s.relayInfo.SubscriptionPostDelta += int64(delta)
	}
	return nil
}

// Refund retains each independent refund before returning from the request.
// A failed funding refund cannot prevent the token refund, or authorize Settle.
func (s *BillingSession) Refund(c *gin.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.settlementID != "" || s.settled {
		return
	}
	if !s.refunded && !s.needsRefundLocked() {
		return
	}
	s.refunded = true
	if s.refundID == "" {
		s.refundID = uuid.NewString()
	}
	var op model.BillingOperation
	switch funding := s.funding.(type) {
	case *WalletFunding:
		op = model.BillingOperation{ID: s.refundID, Kind: "wallet", AccountID: funding.userId, Quota: -funding.consumed, DependsOn: s.reserveRollbackID}
	case *SubscriptionFunding:
		op = model.BillingOperation{ID: s.refundID, Kind: "subscription_refund", RequestID: funding.requestId, DependsOn: s.reserveRollbackID}
	default:
		return
	}
	if err := model.ApplyBillingOperation(op); err != nil {
		logger.LogError(c, common.LogText("error retaining billing refund: %s", err.Error()))
	}
	if s.extraReserved > 0 && s.funding.Source() == BillingSourceSubscription {
		if s.extraRefundID == "" {
			s.extraRefundID = uuid.NewString()
		}
		extra := model.BillingOperation{ID: s.extraRefundID, Kind: "subscription", AccountID: s.relayInfo.SubscriptionId, Quota: -s.extraReserved, DependsOn: s.refundID}
		if err := model.ApplyBillingOperation(extra); err != nil {
			logger.LogError(c, common.LogText("error retaining subscription reserve refund: %s", err.Error()))
		}
	}
	if s.tokenConsumed > 0 && !s.relayInfo.IsPlayground {
		if s.tokenRefundID == "" {
			s.tokenRefundID = uuid.NewString()
		}
		token := model.BillingOperation{ID: s.tokenRefundID, Kind: "token", TokenID: s.relayInfo.TokenId, TokenQuota: -s.tokenConsumed}
		if err := model.ApplyBillingOperation(token); err != nil {
			logger.LogError(c, common.LogText("error retaining token refund: %s", err.Error()))
		}
	}
}

func (s *BillingSession) NeedsRefund() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.needsRefundLocked()
}

func (s *BillingSession) needsRefundLocked() bool {
	if s.settled || s.refunded || s.settlementID != "" {
		return false
	}
	if s.tokenConsumed > 0 {
		return true
	}
	switch funding := s.funding.(type) {
	case *WalletFunding:
		return funding.consumed > 0
	case *SubscriptionFunding:
		return funding.preConsumed > 0 || s.extraReserved > 0
	}
	return false
}

// GetPreConsumedQuota 返回实际预扣的额度。
func (s *BillingSession) GetPreConsumedQuota() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.preConsumedQuota
}

func (s *BillingSession) Reserve(targetQuota int) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	imageRequest := false
	if s.relayInfo != nil {
		_, imageRequest = s.relayInfo.Request.(*dto.ImageRequest)
		imageRequest = imageRequest || s.relayInfo.ImageRequestCount > 0
	}
	if s.settled || s.refunded || s.settlementID != "" || s.trusted && !imageRequest || targetQuota <= s.preConsumedQuota {
		return nil
	}

	if s.reserveRollbackID != "" {
		return errors.New("billing reserve compensation is pending")
	}

	delta := targetQuota - s.preConsumedQuota
	if delta <= 0 {
		return nil
	}

	if err := s.reserveFunding(delta, imageRequest); err != nil {
		return err
	}
	if err := s.reserveToken(delta); err != nil {
		s.rollbackFundingReserve(delta)
		return err
	}

	s.preConsumedQuota += delta
	s.tokenConsumed += delta
	s.extraReserved += delta
	if imageRequest {
		s.trusted = false
	}
	s.syncRelayInfo()
	return nil
}

// ---------------------------------------------------------------------------
// PreConsume — 统一预扣费入口（含信任额度旁路）
// ---------------------------------------------------------------------------

// preConsume 执行预扣费：信任检查 -> 令牌预扣 -> 资金来源预扣。
// 任一步骤失败时原子回滚已完成的步骤。
func (s *BillingSession) preConsume(c *gin.Context, quota int) *types.NewAPIError {
	effectiveQuota := quota

	// ---- 信任额度旁路 ----
	if s.shouldTrust(c) {
		s.trusted = true
		effectiveQuota = 0
		logger.LogInfo(c, common.LogText("user %d has enough quota, trusted without pre-consume (funding=%s)", s.relayInfo.UserId, s.funding.Source()))
	} else if effectiveQuota > 0 {
		logger.LogInfo(c, common.LogText("user %d needs pre-consume %s (funding=%s)", s.relayInfo.UserId, logger.FormatQuota(effectiveQuota), s.funding.Source()))
	}

	// ---- 1) 预扣令牌额度 ----
	if effectiveQuota > 0 {
		if err := PreConsumeTokenQuota(s.relayInfo, effectiveQuota); err != nil {
			return types.NewErrorWithStatusCode(err, types.ErrorCodePreConsumeTokenQuotaFailed, http.StatusForbidden, types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
		}
		s.tokenConsumed = effectiveQuota
	}

	// ---- 2) 预扣资金来源 ----
	if err := s.funding.PreConsume(effectiveQuota); err != nil {
		// This session is discarded on admission failure. Retain the token
		// refund here rather than depending on its caller to call Refund.
		if s.tokenConsumed > 0 && !s.relayInfo.IsPlayground {
			s.tokenRefundID = uuid.NewString()
			refund := model.BillingOperation{ID: s.tokenRefundID, Kind: "token", TokenID: s.relayInfo.TokenId, TokenQuota: -s.tokenConsumed}
			if rollbackErr := model.ApplyBillingOperation(refund); rollbackErr != nil {
				common.SysError(common.LogText("error retaining admission token refund: %s", rollbackErr.Error()))
			}
		}
		// TODO: model 层应定义哨兵错误（如 ErrNoActiveSubscription），用 errors.Is 替代字符串匹配
		if errors.Is(err, ErrInsufficientWalletQuota) {
			userQuota, quotaErr := model.GetUserQuota(s.relayInfo.UserId, false)
			if quotaErr != nil {
				userQuota = 0
			}
			return types.NewErrorWithStatusCode(
				errors.New(i18n.T(c, i18n.MsgQuotaUserInsufficient, map[string]any{"Remaining": logger.FormatQuota(userQuota)})),
				types.ErrorCodeInsufficientUserQuota, http.StatusForbidden,
				types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
		}
		errMsg := err.Error()
		if strings.Contains(errMsg, "no active subscription") || strings.Contains(errMsg, "subscription quota insufficient") {
			return types.NewErrorWithStatusCode(errors.New(i18n.T(c, i18n.MsgQuotaSubscriptionInsufficient, map[string]any{"Error": errMsg})), types.ErrorCodeInsufficientUserQuota, http.StatusForbidden, types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
		}
		return types.NewError(err, types.ErrorCodeUpdateDataError, types.ErrOptionWithSkipRetry())
	}

	s.preConsumedQuota = effectiveQuota

	// ---- 同步 RelayInfo 兼容字段 ----
	s.syncRelayInfo()

	return nil
}

func (s *BillingSession) reserveFunding(delta int, requireAvailableQuota bool) error {
	op := model.BillingOperation{ID: uuid.NewString(), Quota: delta}
	switch funding := s.funding.(type) {
	case *WalletFunding:
		op.Kind, op.AccountID = "wallet", funding.userId
	case *SubscriptionFunding:
		op.Kind, op.AccountID = "subscription", funding.subscriptionId
	default:
		return errors.New("unsupported billing source")
	}
	if err := model.ReserveBillingFunding(op, requireAvailableQuota); err != nil {
		if errors.Is(err, model.ErrBillingInsufficientQuota) {
			return types.NewErrorWithStatusCode(err, types.ErrorCodeInsufficientUserQuota, http.StatusForbidden, types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
		}
		// A lost commit reply must be confirmed/compensated, never replayed.
		s.reserveRollbackID = uuid.NewString()
		rollback := model.BillingOperation{ID: s.reserveRollbackID, Kind: op.Kind, ReverseID: op.ID}
		if retryErr := model.ApplyBillingOperation(rollback); retryErr != nil {
			common.SysError(common.LogText("error retaining reserve compensation: %s", retryErr.Error()))
		} else {
			s.reserveRollbackID = ""
		}
		if op.Kind == "subscription" {
			return types.NewErrorWithStatusCode(
				errors.New(i18n.Translate(s.lang, i18n.MsgQuotaSubscriptionInsufficient, map[string]any{"Error": err.Error()})),
				types.ErrorCodeInsufficientUserQuota, http.StatusForbidden,
				types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
		}
		return types.NewError(err, types.ErrorCodeUpdateDataError, types.ErrOptionWithSkipRetry())
	}
	switch funding := s.funding.(type) {
	case *WalletFunding:
		funding.consumed += delta
	}
	return nil
}

func (s *BillingSession) rollbackFundingReserve(delta int) {
	if s.reserveRollbackID == "" {
		s.reserveRollbackID = uuid.NewString()
	}
	op := model.BillingOperation{ID: s.reserveRollbackID, Quota: -delta}
	switch funding := s.funding.(type) {
	case *WalletFunding:
		op.Kind, op.AccountID = "wallet", funding.userId
		// This refund is now owned by the durable operation, so final Refund
		// only returns the original reservation.
		funding.consumed -= delta
	case *SubscriptionFunding:
		op.Kind, op.AccountID = "subscription", funding.subscriptionId
	}
	if err := model.ApplyBillingOperation(op); err != nil {
		common.SysError(common.LogText("error retaining reserve compensation: %s", err.Error()))
	} else {
		s.reserveRollbackID = ""
	}
}

func (s *BillingSession) reserveToken(delta int) error {
	if delta <= 0 || s.relayInfo.IsPlayground {
		return nil
	}
	if err := PreConsumeTokenQuota(s.relayInfo, delta); err != nil {
		return types.NewErrorWithStatusCode(err, types.ErrorCodePreConsumeTokenQuotaFailed, http.StatusForbidden, types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
	}
	return nil
}

// shouldTrust 统一信任额度检查，适用于钱包和订阅。
func (s *BillingSession) shouldTrust(c *gin.Context) bool {
	// 异步任务（ForcePreConsume=true）必须预扣全额，不允许信任旁路
	if s.relayInfo.ForcePreConsume {
		return false
	}

	trustQuota := operation_setting.GetQuotaSetting().TrustQuotaUSD * common.QuotaPerUnit
	if trustQuota <= 0 || math.IsNaN(trustQuota) || math.IsInf(trustQuota, 0) {
		return false
	}

	// 检查令牌是否充足
	tokenTrusted := s.relayInfo.TokenUnlimited
	if !tokenTrusted {
		tokenQuota := c.GetInt("token_quota")
		tokenTrusted = float64(tokenQuota) > trustQuota
	}
	if !tokenTrusted {
		return false
	}

	switch s.funding.Source() {
	case BillingSourceWallet:
		return float64(s.relayInfo.UserQuota) > trustQuota
	case BillingSourceSubscription:
		// 订阅不能启用信任旁路。原因：
		// 1. PreConsumeUserSubscription 要求 amount>0 来创建预扣记录并锁定订阅
		// 2. SubscriptionFunding.PreConsume 忽略参数，始终用 s.amount 预扣
		// 3. 若信任旁路将 effectiveQuota 设为 0，会导致 preConsumedQuota 与实际订阅预扣不一致
		return false
	default:
		return false
	}
}

// syncRelayInfo 将 BillingSession 的状态同步到 RelayInfo 的兼容字段上。
func (s *BillingSession) syncRelayInfo() {
	info := s.relayInfo
	info.FinalPreConsumedQuota = s.preConsumedQuota
	info.BillingSource = s.funding.Source()

	if sub, ok := s.funding.(*SubscriptionFunding); ok {
		info.SubscriptionId = sub.subscriptionId
		info.SubscriptionPreConsumed = sub.preConsumed + int64(s.extraReserved)
		info.SubscriptionPostDelta = 0
		info.SubscriptionAmountTotal = sub.AmountTotal
		info.SubscriptionAmountUsedAfterPreConsume = sub.AmountUsedAfter + int64(s.extraReserved)
		info.SubscriptionPlanId = sub.PlanId
		info.SubscriptionPlanTitle = sub.PlanTitle
	} else {
		info.SubscriptionId = 0
		info.SubscriptionPreConsumed = 0
	}
}

// ---------------------------------------------------------------------------
// NewBillingSession 工厂 — 根据计费偏好创建会话并处理回退
// ---------------------------------------------------------------------------

// NewBillingSession 根据用户计费偏好创建 BillingSession，处理 subscription_first / wallet_first 的回退。
func NewBillingSession(c *gin.Context, relayInfo *relaycommon.RelayInfo, preConsumedQuota int) (*BillingSession, *types.NewAPIError) {
	if relayInfo == nil {
		return nil, types.NewError(fmt.Errorf("relayInfo is nil"), types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}
	if relayInfo.UserId <= 0 {
		return nil, types.NewError(fmt.Errorf("invalid billing user"), types.ErrorCodeInvalidRequest, types.ErrOptionWithSkipRetry())
	}

	if err := model.RecoverBillingAccount(relayInfo.UserId, relayInfo.TokenId); err != nil {
		return nil, types.NewError(err, types.ErrorCodeQueryDataError, types.ErrOptionWithSkipRetry())
	}

	pref := common.NormalizeBillingPreference(relayInfo.UserSetting.BillingPreference)

	// 钱包路径需要先检查用户额度
	tryWallet := func() (*BillingSession, *types.NewAPIError) {
		userQuota, err := model.GetUserQuota(relayInfo.UserId, false)
		if err != nil {
			return nil, types.NewError(err, types.ErrorCodeQueryDataError, types.ErrOptionWithSkipRetry())
		}
		if userQuota <= 0 {
			return nil, types.NewErrorWithStatusCode(
				errors.New(i18n.T(c, i18n.MsgQuotaUserInsufficient, map[string]any{"Remaining": logger.FormatQuota(userQuota)})),
				types.ErrorCodeInsufficientUserQuota, http.StatusForbidden,
				types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
		}
		if userQuota-preConsumedQuota < 0 {
			return nil, types.NewErrorWithStatusCode(
				errors.New(i18n.T(c, i18n.MsgQuotaPreConsumeInsufficient, map[string]any{"Remaining": logger.FormatQuota(userQuota), "Required": logger.FormatQuota(preConsumedQuota)})),
				types.ErrorCodeInsufficientUserQuota, http.StatusForbidden,
				types.ErrOptionWithSkipRetry(), types.ErrOptionWithNoRecordErrorLog())
		}
		relayInfo.UserQuota = userQuota

		session := &BillingSession{
			relayInfo: relayInfo,
			funding:   &WalletFunding{userId: relayInfo.UserId},
			lang:      i18n.StatedLang(c),
		}
		if apiErr := session.preConsume(c, preConsumedQuota); apiErr != nil {
			return nil, apiErr
		}
		return session, nil
	}

	trySubscription := func() (*BillingSession, *types.NewAPIError) {
		subConsume := int64(preConsumedQuota)
		if subConsume <= 0 {
			subConsume = 1
		}
		session := &BillingSession{
			relayInfo: relayInfo,
			lang:      i18n.StatedLang(c),
			funding: &SubscriptionFunding{
				requestId: relayInfo.RequestId,
				userId:    relayInfo.UserId,
				modelName: relayInfo.GetBillingModelName(),
				amount:    subConsume,
			},
		}
		// 必须传 subConsume 而非 preConsumedQuota，保证 SubscriptionFunding.amount、
		// preConsume 参数和 FinalPreConsumedQuota 三者一致，避免订阅多扣费。
		if apiErr := session.preConsume(c, int(subConsume)); apiErr != nil {
			return nil, apiErr
		}
		return session, nil
	}

	switch pref {
	case "subscription_only":
		return trySubscription()
	case "wallet_only":
		return tryWallet()
	case "wallet_first":
		session, err := tryWallet()
		if err != nil {
			if err.GetErrorCode() == types.ErrorCodeInsufficientUserQuota {
				return trySubscription()
			}
			return nil, err
		}
		return session, nil
	case "subscription_first":
		fallthrough
	default:
		hasSub, subCheckErr := model.HasActiveUserSubscription(relayInfo.UserId)
		if subCheckErr != nil {
			return nil, types.NewError(subCheckErr, types.ErrorCodeQueryDataError, types.ErrOptionWithSkipRetry())
		}
		if !hasSub {
			return tryWallet()
		}
		session, apiErr := trySubscription()
		if apiErr != nil {
			if apiErr.GetErrorCode() == types.ErrorCodeInsufficientUserQuota {
				// 仅当用户的活跃订阅允许钱包回退时才回退到钱包，否则返回订阅额度不足错误
				allowOverflow, overflowErr := model.UserActiveSubscriptionsAllowWalletOverflow(relayInfo.UserId)
				if overflowErr != nil {
					return nil, types.NewError(overflowErr, types.ErrorCodeQueryDataError, types.ErrOptionWithSkipRetry())
				}
				if allowOverflow {
					return tryWallet()
				}
				return nil, apiErr
			}
			return nil, apiErr
		}
		return session, nil
	}
}
