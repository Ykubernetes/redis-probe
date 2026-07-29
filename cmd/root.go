package cmd

import (
    "fmt"
    "os"
    "time"

    "github.com/Ykubernetes/redis-probe/internal/monitor"

    "github.com/spf13/cobra"
)

var (
    addr           string
    username       string
    password       string
    db             int
    pingInterval   time.Duration
    connectTimeout time.Duration
    cmdTimeout     time.Duration
    maxRetries     int
    logFile        string
    successTimes   int
    latencyWarn    time.Duration
    heartbeatLog   bool
)

var rootCmd = &cobra.Command{
    Use:   "redis-probe",
    Short: "探测带 Proxy 的 Redis 读写中断情况（支持单机/主从/集群）",
    Long: `专门针对前面有 Proxy 的 Redis 架构。
分别统计「写中断时长」和「读中断时长」，适合主从切换、重启、升配等场景。`,
    Run: func(cmd *cobra.Command, args []string) {
        if logFile == "" {
            // 格式：redis-probe-2026-07-29_15-23-45.log
            logFile = fmt.Sprintf("redis-probe-%s.log", time.Now().Format("2006-01-02_15-04-05"))
        }

        cfg := monitor.Config{
            Addr:           addr,
            Username:       username,
            Password:       password,
            DB:             db,
            PingInterval:   pingInterval,
            ConnectTimeout: connectTimeout,
            CmdTimeout:     cmdTimeout,
            MaxRetries:     maxRetries,
            LogFile:        logFile,
            SuccessTimes:   successTimes,
            LatencyWarn:    latencyWarn,
            HeartbeatLog:   heartbeatLog,
        }
        monitor.Run(cfg)
    },
}

func init() {
    rootCmd.Flags().StringVarP(&addr, "addr", "a", "", "Redis Proxy VIP 地址（必填）")
    rootCmd.Flags().StringVarP(&username, "username", "u", "", "用户名")
    rootCmd.Flags().StringVarP(&password, "password", "p", "", "密码")
    rootCmd.Flags().IntVarP(&db, "db", "d", 0, "数据库编号")
    rootCmd.Flags().DurationVarP(&pingInterval, "interval", "i", 2*time.Second, "探测间隔")
    rootCmd.Flags().DurationVarP(&connectTimeout, "timeout", "t", 5*time.Second, "连接超时")
    rootCmd.Flags().DurationVar(&cmdTimeout, "cmd-timeout", 2*time.Second, "单次命令超时")
    rootCmd.Flags().IntVarP(&maxRetries, "retries", "r", -1, "客户端内部重试次数（-1 关闭）")
    rootCmd.Flags().StringVarP(&logFile, "log", "l", "", "日志文件（默认带日期）")
    rootCmd.Flags().IntVarP(&successTimes, "success-times", "s", 3, "连续成功多少次才算真正恢复")
    rootCmd.Flags().DurationVar(&latencyWarn, "latency-warn", 500*time.Millisecond, "延迟告警阈值（0 关闭）")
    rootCmd.Flags().BoolVar(&heartbeatLog, "heartbeat", false, "正常时打印心跳")

    _ = rootCmd.MarkFlagRequired("addr")
}

func Execute() {
    if err := rootCmd.Execute(); err != nil {
        fmt.Println(err)
        os.Exit(1)
    }
}
