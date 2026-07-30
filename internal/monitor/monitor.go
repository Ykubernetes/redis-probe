package monitor

import (
    "context"
    "errors"
    "fmt"
    "io"
    "log"
    "net"
    "os"
    "os/signal"
    "strings"
    "sync/atomic"
    "syscall"
    "time"

    "github.com/redis/go-redis/v9"
)

type Config struct {
    Addr           string
    Username       string
    Password       string
    DB             int
    PingInterval   time.Duration
    ConnectTimeout time.Duration
    CmdTimeout     time.Duration
    MaxRetries     int
    LogFile        string
    SuccessTimes   int
    LatencyWarn    time.Duration
    HeartbeatLog   bool
}

type pathState struct {
    name         string // "写" 或 "读"
    isOK         bool
    everOK       bool
    downSince    time.Time
    successCount int
    recoverCount int
}

// dialEvent 记录连接重建事件
// go-redis 连接池在发现旧连接不健康时会自动创建新连接（触发 DialHook）
// 通过追踪拨号次数变化，可以感知到连接曾经断开过——即使命令最终执行成功
type dialEvent struct {
    count       int64         // 总拨号次数（原子操作）
    lastTime    time.Time     // 最后一次拨号时间
    lastErr     error         // 最后一次拨号错误
    dialCost    time.Duration // 最后一次拨号耗时
    initialized int64         // 基线是否已建立（0=未建立，1=已建立）
}

func Run(cfg Config) {
    if cfg.SuccessTimes <= 0 {
        cfg.SuccessTimes = 3
    }
    if cfg.CmdTimeout <= 0 {
        cfg.CmdTimeout = 2 * time.Second
    }

    logger, logFile, err := newLogger(cfg.LogFile)
    if err != nil {
        log.Fatalf("初始化日志失败: %v", err)
    }
    if logFile != nil {
        defer logFile.Close()
    }

    logger.Printf("启动 Redis-Probe（读写分离统计 | Proxy 架构 | 连接重建追踪）")
    logger.Printf("  地址            : %s", cfg.Addr)
    logger.Printf("  用户名          : %s", cfg.Username)
    logger.Printf("  数据库          : %d", cfg.DB)
    logger.Printf("  探测间隔        : %v", cfg.PingInterval)
    logger.Printf("  连接超时        : %v", cfg.ConnectTimeout)
    logger.Printf("  命令超时        : %v", cfg.CmdTimeout)
    logger.Printf("  客户端重试      : %d", cfg.MaxRetries)
    logger.Printf("  恢复成功阈值  : %d 次", cfg.SuccessTimes)
    logger.Printf("  延迟告警阈值  : %v", cfg.LatencyWarn)
    logger.Printf("  心跳日志        : %v", cfg.HeartbeatLog)
    logger.Printf("  日志文件        : %s", cfg.LogFile)

    // 连接重建事件追踪器
    dialTracker := &dialEvent{}

    rdb := newClient(cfg, dialTracker, logger)
    defer rdb.Close()

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    go func() {
        sigCh := make(chan os.Signal, 1)
        signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
        <-sigCh
        logger.Println("收到退出信号，正在退出...")
        cancel()
    }()

    writeState := &pathState{name: "写"}
    readState := &pathState{name: "读"}

    // 先建立初始连接，避免首次拨号被误判为"连接重建"
    // go-redis 默认懒连接，newClient 不会立即拨号
    // 如果不先 Ping，第一次 checkReadWrite 的拨号会被误判为连接重建
    pingCtx, pingCancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
    if err := rdb.Ping(pingCtx).Err(); err != nil {
        logger.Printf("初始连接建立失败: %v（将在探测循环中继续重试）", err)
    } else {
        logger.Printf("初始连接建立成功")
    }
    pingCancel()

    // 在初始连接建立后采集基线，后续拨号次数增加才意味着连接重建
    initialDialCount := atomic.LoadInt64(&dialTracker.count)
    logger.Printf("  初始拨号计数    : %d", initialDialCount)

    // 标记基线已建立，此后 DialHook 的拨号才打印"连接重建"日志
    atomic.StoreInt64(&dialTracker.initialized, 1)

    for {
        select {
        case <-ctx.Done():
            logger.Println("探测已停止")
            return
        default:
        }

        start := time.Now()
        writeErr, readErr := checkReadWrite(ctx, rdb, cfg.CmdTimeout)
        cost := time.Since(start)

        // 检查连接是否发生了重建
        // go-redis 连接池在发现旧连接不健康时会自动创建新连接（DialHook 触发）
        // 这意味着即使命令执行成功，连接也可能曾经断开过
        currentDialCount := atomic.LoadInt64(&dialTracker.count)
        if currentDialCount > initialDialCount {
            rebuildCount := currentDialCount - initialDialCount
            dialCost := dialTracker.dialCost
            lastDialErr := dialTracker.lastErr
            logger.Printf("【连接重建】检测到 %d 次拨号事件 | 最近拨号耗时: %v | 拨号错误: %v",
                rebuildCount, dialCost, lastDialErr)
            initialDialCount = currentDialCount

            // 即使当前命令成功了，也记录一次连接重建事件
            // 因为这证明连接曾经断开过，只是连接池自动恢复了
            if writeState.isOK && readState.isOK && writeErr == nil && readErr == nil {
                logger.Printf("【连接重建注意】命令执行成功，但连接发生过重建，存在短暂中断（被连接池自动恢复）")
            }
        }

        // 连接级严重错误时，主动重建连接
        if writeErr != nil && isConnError(writeErr) || readErr != nil && isConnError(readErr) {
            _ = rdb.Close()
            rdb = newClient(cfg, dialTracker, logger)
        }

        handlePath(logger, writeState, writeErr, cost, cfg)
        handlePath(logger, readState, readErr, cost, cfg)

        // 正常心跳（读写都 OK 时）
        if cfg.HeartbeatLog && writeState.isOK && readState.isOK {
            logger.Printf("【心跳正常】写+读均可用 | 耗时: %v", cost)
        }

        // 延迟告警
        if cfg.LatencyWarn > 0 && cost > cfg.LatencyWarn && writeState.isOK && readState.isOK {
            logger.Printf("【延迟告警】总耗时 %v 超过阈值 %v", cost, cfg.LatencyWarn)
        }

        select {
        case <-ctx.Done():
            return
        case <-time.After(cfg.PingInterval):
        }
    }
}

func handlePath(logger *log.Logger, st *pathState, err error, cost time.Duration, cfg Config) {
    if err != nil {
        st.successCount = 0
        if st.isOK {
            st.downSince = time.Now()
            st.isOK = false
            logger.Printf("【%s中断】%v | 耗时: %v", st.name, err, cost)
        } else if !st.everOK {
            logger.Printf("【%s失败】%v | 耗时: %v", st.name, err, cost)
        } else {
            logger.Printf("【%s恢复中】仍失败: %v | 耗时: %v", st.name, err, cost)
        }
        return
    }

    // 成功
    if !st.everOK {
        logger.Printf("【%s首次成功】耗时: %v", st.name, cost)
        st.everOK = true
        st.isOK = true
        return
    }

    if !st.isOK {
        st.successCount++
        if st.successCount >= cfg.SuccessTimes {
            downtime := time.Since(st.downSince)
            st.recoverCount++
            logger.Printf("【%s恢复成功】第 %d 次 | %s中断时长: %.3f 秒 | 耗时: %v",
                st.name, st.recoverCount, st.name, downtime.Seconds(), cost)
            st.isOK = true
            st.downSince = time.Time{}
            st.successCount = 0
        } else {
            logger.Printf("【%s恢复中】连续成功 %d/%d | 耗时: %v",
                st.name, st.successCount, cfg.SuccessTimes, cost)
        }
    }
}

// connRebuildHook 通过 DialHook 追踪连接重建事件
// go-redis 的连接池在发现旧连接不健康时会自动调用 Dial 创建新连接
// 通过 Hook 这个 Dial 过程，我们可以感知到连接曾经断开过
type connRebuildHook struct {
    tracker *dialEvent
    logger  *log.Logger
}

func (h *connRebuildHook) DialHook(next redis.DialHook) redis.DialHook {
    return func(ctx context.Context, network, addr string) (net.Conn, error) {
        count := atomic.AddInt64(&h.tracker.count, 1)
        dialStart := time.Now()

        conn, err := next(ctx, network, addr)
        dialCost := time.Since(dialStart)

        h.tracker.lastTime = time.Now()
        h.tracker.dialCost = dialCost
        h.tracker.lastErr = err

        if count > 1 {
            // 只在基线建立后才打印重建日志
            // 基线建立前的多次拨号是连接池预热（MinIdleConns），不是重建
            if atomic.LoadInt64(&h.tracker.initialized) == 1 {
                if err != nil {
                    h.logger.Printf("【连接重建-失败】第 %d 次拨号 | 耗时: %v | 错误: %v", count, dialCost, err)
                } else {
                    h.logger.Printf("【连接重建-成功】第 %d 次拨号 | 耗时: %v", count, dialCost)
                }
            }
        }

        return conn, err
    }
}

func (h *connRebuildHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
    return func(ctx context.Context, cmd redis.Cmder) error {
        return next(ctx, cmd)
    }
}

func (h *connRebuildHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
    return func(ctx context.Context, cmds []redis.Cmder) error {
        return next(ctx, cmds)
    }
}

func newClient(cfg Config, dialTracker *dialEvent, logger *log.Logger) *redis.Client {
    client := redis.NewClient(&redis.Options{
        Addr:         cfg.Addr,
        Username:     cfg.Username,
        Password:     cfg.Password,
        DB:           cfg.DB,
        DialTimeout:  cfg.ConnectTimeout,
        ReadTimeout:  cfg.CmdTimeout,
        WriteTimeout: cfg.CmdTimeout,
        MaxRetries:   cfg.MaxRetries,
        PoolSize:     2,
        MinIdleConns: 1,
        // 关闭拨号重试
        // 默认 DialerRetries=5, DialerRetryTimeout=100ms
        // go-redis 在创建新连接失败时会自动重试 5 次（总计约 500ms）
        // 这会掩盖短暂的中断——拨号失败后重试成功了，探测就看不到错误
        // 设置为 1 次拨号，不重试，让拨号失败立即暴露
        DialerRetries:      1,
        DialerRetryTimeout: 0,
    })

    // DialHook 追踪连接重建
    client.AddHook(&connRebuildHook{
        tracker: dialTracker,
        logger:  logger,
    })

    return client
}

// checkReadWrite 分别检测写和读
// 返回 (writeErr, readErr)
func checkReadWrite(ctx context.Context, rdb *redis.Client, timeout time.Duration) (error, error) {
    c, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()

    testKey := "__redis_probe_rw__"
    testVal := fmt.Sprintf("%d", time.Now().UnixNano())

    var writeErr, readErr error

    // 写探测
    if err := rdb.Set(c, testKey, testVal, 30*time.Second).Err(); err != nil {
        writeErr = fmt.Errorf("SET: %w", err)
    }

    // 读探测
    // 即使写失败，也尝试读（可能读的是旧值或从库）
    val, err := rdb.Get(c, testKey).Result()
    if err != nil {
        // redis.Nil（key 不存在）不是读中断
        // 当写失败（如 READONLY）时 key 不会被写入，GET 自然返回 redis.Nil
        // 这说明读路径本身是正常的，只是 key 不存在，不应报读中断
        if errors.Is(err, redis.Nil) {
            if writeErr != nil {
                // 写失败导致 key 不存在，读路径正常，不报读错误
                readErr = nil
            } else {
                // 写成功但 key 不存在，说明读到了旧数据或从库未同步——读异常
                readErr = fmt.Errorf("GET: key 不存在（写成功但读不到，可能主从延迟）: %w", err)
            }
        } else {
            // 真正的读错误（连接断开、超时等）
            readErr = fmt.Errorf("GET: %w", err)
        }
        // ============================================================
    } else if writeErr == nil && val != testVal {
        // 写成功但读到的值不对，也视为读异常
        readErr = fmt.Errorf("GET 值不匹配 want=%s got=%s", testVal, val)
    }

    // 清理（写成功时才删）
    if writeErr == nil {
        _ = rdb.Del(c, testKey).Err()
    }

    return writeErr, readErr
}

func isConnError(err error) bool {
    if err == nil {
        return false
    }
    s := err.Error()
    return strings.Contains(s, "EOF") ||
        strings.Contains(s, "connection refused") ||
        strings.Contains(s, "broken pipe") ||
        strings.Contains(s, "i/o timeout") ||
        strings.Contains(s, "connect: connection")
}

func newLogger(path string) (*log.Logger, *os.File, error) {
    writers := []io.Writer{os.Stdout}
    var file *os.File
    if path != "" {
        f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
        if err != nil {
            return nil, nil, fmt.Errorf("打开日志文件失败: %w", err)
        }
        file = f
        writers = append(writers, f)
    }
    multi := io.MultiWriter(writers...)
    return log.New(multi, "", log.LstdFlags|log.Lmicroseconds), file, nil
}
