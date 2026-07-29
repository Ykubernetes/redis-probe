package monitor

import (
    "context"
    "fmt"
    "io"
    "log"
    "os"
    "os/signal"
    "strings"
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

    logger.Printf("启动 Redis-Probe（读写分离统计 | Proxy 架构）")
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

    rdb := newClient(cfg)
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

        // 连接级严重错误时，主动重建连接
        if writeErr != nil && isConnError(writeErr) || readErr != nil && isConnError(readErr) {
            _ = rdb.Close()
            rdb = newClient(cfg)
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

func newClient(cfg Config) *redis.Client {
    return redis.NewClient(&redis.Options{
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
    })
}

// checkReadWrite 分别检测写和读
// 返回 (writeErr, readErr)
func checkReadWrite(ctx context.Context, rdb *redis.Client, timeout time.Duration) (error, error) {
    c, cancel := context.WithTimeout(ctx, timeout)
    defer cancel()

    testKey := "__redis_probe_rw__"
    testVal := fmt.Sprintf("%d", time.Now().UnixNano())

    var writeErr, readErr error

    // ---- 写探测 ----
    if err := rdb.Set(c, testKey, testVal, 30*time.Second).Err(); err != nil {
        writeErr = fmt.Errorf("SET: %w", err)
    }

    // ---- 读探测 ----
    // 即使写失败，也尝试读（可能读的是旧值或从库）
    val, err := rdb.Get(c, testKey).Result()
    if err != nil {
        readErr = fmt.Errorf("GET: %w", err)
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
