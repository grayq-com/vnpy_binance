# binance_usdt_gateway.py 说明文档

> VeighNa (vnpy) 框架下的 Binance USDT 永续合约交易网关实现

## 1. 文件概述

`binance_usdt_gateway.py` 是 VeighNa 框架对接币安 (Binance) **正向合约 (USDT 永续)** 的核心网关文件,职责:

1. **REST 通信** — 下单、撤单、查询 (账户/持仓/委托/合约/历史K线)
2. **WebSocket 行情** — 实时 ticker + 深度 + 永续 1 分钟 K 线
3. **WebSocket 交易** — 账户、持仓、订单回报推送
4. **签名鉴权** — HMAC-SHA256 + 服务端时间同步
5. **会话保活** — listenKey 续期、断线自动重连

### 核心约束

```
1. 只支持全仓模式 (Cross Margin)
2. 只支持单向持仓模式 (Single-Side / Hedge = False)
3. 只支持正向合约 (USDT 计价的 USDT-M)
```

启动时会调用 `set_position_mode(False)` 强制设为单向持仓模式。

---

## 2. 架构总览

```
┌────────────────────────────────────────────────────────────────┐
│                    BinanceUsdtGateway                          │
│                  (对外接口 / BaseGateway)                       │
├──────────────┬──────────────────────┬──────────────────────────┤
│  rest_api    │   trade_ws_api       │   market_ws_api          │
│ (REST 客户端) │ (交易 WS 客户端)      │  (行情 WS 客户端)         │
├──────────────┼──────────────────────┼──────────────────────────┤
│ /fapi/v1/*   │  ACCOUNT_UPDATE      │  @ticker                 │
│ /fapi/v2/*   │  ORDER_TRADE_UPDATE  │  @depth5                 │
│              │  listenKeyExpired    │  @continuousKline_1m     │
└──────┬───────┴──────────┬───────────┴────────────┬─────────────┘
       │                  │                        │
       │  HTTPS + 签名     │  WSS + listenKey       │  WSS 公共流
       ▼                  ▼                        ▼
   fapi.binance.com    fstream.binance.com/ws   fstream.binance.com/stream
```

**事件总线**: 网关通过 `gateway.on_tick / on_order / on_trade / on_account / on_position / on_contract` 把数据推送到 vnpy 的 EventEngine。

---

## 3. 三大核心组件

### 3.1 BinanceUsdtGateway (主网关)

继承 `BaseGateway`,作为对外接口的统一入口。

**默认配置**:
```python
default_setting = {
    "key": "",
    "secret": "",
    "服务器": ["REAL", "TESTNET"],
    "代理地址": "",
    "代理端口": 0,
}
```

**主要方法**:

| 方法 | 作用 |
|------|------|
| `connect(setting)` | 启动 REST + 行情 WS,注册定时器,设置单向持仓 |
| `subscribe(req)` | 订阅合约行情 |
| `send_order(req)` | 委托下单 (委托 REST) |
| `cancel_order(req)` | 委托撤单 |
| `query_history(req)` | 拉取历史 K 线 |
| `process_timer_event(event)` | 每 3 分钟定时查询账户/持仓,每次 tick 调用 `keep_user_stream()` |
| `on_order(order)` | 订单回报处理 (后台线程异步) |
| `get_order(orderid)` | 查询本地缓存的订单 |
| `close()` | 关闭 REST + 两个 WS |

**订单回报异步处理** (`on_order` 方法):

- 启动后台 daemon 线程处理,避免阻塞 WebSocket 网络回调
- 维护 `self.orders[orderid]` 缓存,用于增量成交计算 (`traded = order.traded - last_order.traded`)
- 增量成交 > 0 时,合成 `TradeData` 并回调 `super().on_trade(trade)`
- 若 `traded < 0`,说明 last_order 状态滞后,直接丢弃此次推送
- 异常被吞掉并打印 (避免线程崩溃影响后续处理)

### 3.2 BinanceUsdtRestApi (REST 客户端)

继承 `RestClient`,负责所有 HTTP 请求。

#### 3.2.1 签名机制 (`sign` 方法)

```python
security == Security.SIGNED:
    timestamp = int(time.time() * 1000) - abs(time_offset)
    query = urlencode(sorted(params.items()))
    signature = hmac.new(secret, query, hashlib.sha256).hexdigest()
    path = path + "?" + query + "&signature=" + signature
```

**三种鉴权级别**:
| Security | 用途 | 是否需要签名 |
|----------|------|-------------|
| `NONE` | 公开接口 (合约信息/时间/K线) | 否 |
| `API_KEY` | listenKey 相关 | 仅头部带 X-MBX-APIKEY |
| `SIGNED` | 交易/账户/持仓 | 完整 HMAC 签名 |

**关键点**:
- `time_offset` 来自 `query_time()`,校正本地与服务端时间差,避免 `-1021 timestamp` 错误
- 参数必须 `sorted()` 后签名,否则验签失败
- secret 在 `connect()` 中已 `encode()` 为 bytes

#### 3.2.2 主要 REST 端点

| 接口 | 路径 | 鉴权 |
|------|------|------|
| 设置持仓模式 | `POST /fapi/v1/positionSide/dual` | SIGNED |
| 设置杠杆 | `POST /fapi/v1/leverage` | SIGNED |
| 查询时间 | `GET /fapi/v1/time` | NONE |
| 查询账户 | `GET /fapi/v2/account` | SIGNED |
| 查询持仓 | `GET /fapi/v2/positionRisk` | SIGNED |
| 查询未成交 | `GET /fapi/v1/openOrders` | SIGNED |
| 查询合约 | `GET /fapi/v1/exchangeInfo` | NONE |
| 下单 | `POST /fapi/v1/order` | SIGNED |
| 撤单 | `DELETE /fapi/v1/order` | SIGNED |
| listenKey 申请 | `POST /fapi/v1/listenKey` | API_KEY |
| listenKey 续期 | `PUT /fapi/v1/listenKey` | API_KEY |
| 历史 K 线 | `GET /fapi/v1/continuousKlines` | NONE |

#### 3.2.3 订单号生成

```python
connect_time = int(datetime.now().strftime("%m%d%H%M")) * 1000
order_count = 1000  # 起始递增计数
orderid = str(connect_time + self._new_order_id())
```

- `connect_time` 用月日时分 × 1000 作为基础 (例如 0614 21:19 → 6142119 × 1000)
- `_new_order_id()` 通过 `order_count_lock` 保证线程安全的递增
- 最终作为 `newClientOrderId` 发给币安,后续撤单/查询都通过它索引

#### 3.2.4 listenKey 保活 (`keep_user_stream`)

```python
keep_alive_count += 1
if keep_alive_count < 600:   # 等累计到 600 次定时事件才续期
    return None
# 发 PUT /fapi/v1/listenKey
```

- 定时器 `process_timer_event` 大约每秒触发一次,累计 600 次 (≈10 分钟) 续期一次
- 币安要求 30 分钟内续期,这里冗余设置足够安全
- 失败重试机制: 连续失败 ≤3 次重新续期,否则重新申请 listenKey

#### 3.2.5 时间戳错误恢复 (`failed_with_timestamp`)

```python
if resp.get('code') == -1021:    # 币安时间戳错误
    threading.Thread(target=self.query_time, daemon=True).start()
```

异步重新同步服务端时间,避免阻塞当前回调。

#### 3.2.6 历史 K 线拉取 (`query_history`)

- 单次拉取上限 `limit = 1500` 根
- 循环调用,以 `bar.datetime + interval` 作为下一批 `startTime`
- 每批间隔 `sleep(0.5)` 防止触发限频
- 接口使用 `/fapi/v1/continuousKlines` (永续连续 K 线),`contractType=PERPETUAL`

### 3.3 BinanceUsdtTradeWebsocketApi (交易 WS)

继承 `WebsocketClient`,监听账户、订单、listenKey 过期事件。

**连接参数**:
```python
ping_interval = 25      # 每 25 秒发 ping
receive_timeout = 40    # 40 秒无消息判定超时
```

**事件分发** (`on_packet`):

| packet["e"] | 处理函数 |
|-------------|---------|
| `ACCOUNT_UPDATE` | `on_account` — 推送账户资金 + 持仓 |
| `ORDER_TRADE_UPDATE` | `on_order` — 推送订单状态 |
| `listenKeyExpired` | `on_listen_key_expired` — 主动断开触发重连 |

**关键设计**:
- `on_disconnected` 中调用 `start_user_stream()` 重新申请 listenKey 并重连
- `disconnect()` 通过 `run_coroutine_threadsafe` 安全关闭 asyncio loop 内的 ws
- `on_account` 同步推送账户和持仓 (其中 BOTH 方向表示单向持仓)
- `on_order` 中订单拒单 (`X == "REJECTED"`) 会用 `setattr(order, "rejected_reason", ...)` 把拒单原因动态写入订单对象

**持仓方向处理**:
```python
volume = float(pos_data["pa"])
if volume > 0: direction = LONG
elif volume < 0: direction = SHORT
else:
    # 即使持仓为 0,也推送 on_position
    # 这样策略可以同步持仓状态,清除 pos_data 中的旧数据
    position = PositionData(..., volume=0, ...)
    self.gateway.on_position(position)
```

零持仓也推送是**重要修复点** — 否则策略平仓后理论持仓无法清零。

### 3.4 BinanceUsdtDataWebsocketApi (行情 WS)

继承 `WebsocketClient`,订阅合约实时行情。

#### 3.4.1 订阅频道

每个 symbol 订阅三个流:
```
{symbol}_perpetual@continuousKline_1m   # 永续 1 分钟 K 线
{symbol}@ticker                          # 24h ticker
{symbol}@depth5                          # 5 档深度
```

#### 3.4.2 数据处理 (`process_tick_data`)

**异步处理**: `on_packet` 把数据丢到 daemon 线程处理,避免阻塞 WS 接收。

- `ticker` 流:更新 `last_price / volume / turnover / open_price / high_price / low_price`
- `depth5` 流:更新 `bid_price_1..5 / bid_volume_1..5 / ask_price_1..5 / ask_volume_1..5`
- `continuousKline_1m` 流:把 K 线 OHLCV 写入 tick 的扩展字段 (`cKline_open / cKline_close / cKline_1m / cKdatetime` 等)

**tick 推送频率**:

> **关键设计决策**: 此网关采用 **每 tick 立即推送** 策略 (而非按计数器或时间窗口节流)。
>
> 历史代码中曾有计数器方案 (`tick_counter / push_threshold`) 和时间窗口方案 (`last_push_time`),但当前实现均已注释。注释代码保留作为可选节流参考。

**重连后重新订阅**: `on_connected` 触发时,会基于 `self.ticks.keys()` 把所有已订阅的合约重新发 `SUBSCRIBE`,实现自动重订阅。

---

## 4. 关键常量与映射

### 4.1 主机地址

| 用途 | 地址 |
|------|------|
| 实盘 REST | `https://fapi.binance.com` |
| 实盘 交易 WS | `wss://fstream.binance.com/ws/` |
| 实盘 行情 WS | `wss://fstream.binance.com/stream` |
| 模拟盘 REST | `https://testnet.binancefuture.com` |
| 模拟盘 交易 WS | `wss://stream.binancefuture.com/ws/` |
| 模拟盘 行情 WS | `wss://stream.binancefuture.com/stream` |

### 4.2 状态/类型/方向映射

```python
STATUS_BINANCES2VT = {
    "NEW":              Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED":           Status.ALLTRADED,
    "CANCELED":         Status.CANCELLED,
    "REJECTED":         Status.REJECTED,
    "EXPIRED":          Status.CANCELLED,
}

ORDERTYPE_VT2BINANCES = {
    OrderType.LIMIT:  ("LIMIT",  "GTC"),
    OrderType.MARKET: ("MARKET", "GTC"),
    OrderType.FAK:    ("LIMIT",  "IOC"),
    OrderType.FOK:    ("LIMIT",  "FOK"),
    OrderType.STOP:   ("STOP",   "GTC"),
}

DIRECTION_VT2BINANCES = {
    Direction.LONG:  "BUY",
    Direction.SHORT: "SELL",
}

INTERVAL_VT2BINANCES = {
    Interval.MINUTE: "1m",
    Interval.HOUR:   "1h",
    Interval.DAILY:  "1d",
}
```

### 4.3 全局缓存

- `symbol_contract_map: dict[str, ContractData]` — 全局合约信息缓存 (跨策略共享)
- `VTTRADE_ID` — 当前最新成交 ID (作为 TradeData 的 tradeid 占位符,实际由 `gateway.on_order` 中增量计算生成 TradeData)

---

## 5. 完整数据流

### 5.1 启动流程

```
gateway.connect(setting)
    │
    ├─► rest_api.connect()
    │     ├─► init(host, proxy) + start()
    │     ├─► query_time()           # 同步时间偏移
    │     ├─► query_contract()       # 拉取合约信息 → symbol_contract_map
    │     ├─► query_account()
    │     ├─► query_position()
    │     ├─► query_order()
    │     └─► start_user_stream()    # 申请 listenKey → 连接 trade_ws_api
    │
    ├─► market_ws_api.connect()      # 连接行情 WS
    │
    └─► event_engine.register(EVENT_TIMER, process_timer_event)
```

### 5.2 下单流程

```
策略 → gateway.send_order(req)
    │
    ├─► rest_api.send_order(req)
    │     ├─► 生成 orderid = connect_time + atomic counter
    │     ├─► 构造 OrderData → gateway.on_order()  # 先登记本地
    │     ├─► POST /fapi/v1/order
    │     ├─► on_send_order()           # 同步回报 → 更新 traded/price/status
    │     ├─► on_send_order_failed()    # 异步线程处理拒单 → on_order(REJECTED)
    │     └─► on_send_order_error()     # 网络异常 → on_order(REJECTED)
    │
    └─► (异步) WebSocket ORDER_TRADE_UPDATE
          └─► trade_ws_api.on_order(packet)
                ├─► 解析 ord_data
                ├─► 处理拒单原因 (setattr rejected_reason)
                └─► gateway.on_order(order)
                      └─► 后台线程: 计算增量 traded → 合成 TradeData → on_trade()
```

### 5.3 行情推送流程

```
Binance → market_ws_api.on_packet(packet)
    │
    ├─► 若是 ping → 回 pong
    │
    └─► daemon 线程: process_tick_data(packet)
          ├─► 解析 stream → symbol + channel
          ├─► 根据 channel 更新 TickData (ticker/depth5/kline_1m)
          └─► 若 last_price 存在 → gateway.on_tick(copy(tick))
                └─► EventEngine → Strategy.on_tick()
```

### 5.4 定时事件流程

```
EVENT_TIMER (每秒)
    │
    └─► gateway.process_timer_event(event)
          ├─► rest_api.keep_user_stream()  # 累计 600 次才真正 PUT
          ├─► timer_count += 1
          └─► timer_count >= 180 (3 分钟):
                ├─► rest_api.query_account()
                └─► rest_api.query_position()
```

---

## 6. 修改注意事项

### 6.1 不可破坏的设计

1. **自动重连机制** — `on_disconnected` 必须触发 `start_user_stream()` 重新申请 listenKey,这是实盘稳定性的关键。
2. **ping/pong 心跳** — 25 秒 ping + 40 秒超时,改动需评估是否会触发币安断连。
3. **订单回报异步处理** — 必须在 daemon 线程处理,否则会阻塞 WS 接收循环。
4. **签名参数排序** — `sorted(params.items())` 顺序敏感,不能改。
5. **time_offset 校正** — 任何时区/时间相关改动都要确保 `query_time()` 仍能正常同步。

### 6.2 已知设计选择

| 设计 | 当前实现 | 备选方案 |
|------|---------|---------|
| Tick 推送频率 | 每 tick 立即推送 | 计数器节流 / 时间窗口节流 (已注释保留) |
| 订单回调 | 异步线程 | 同步处理 (会阻塞 WS) |
| 持仓同步 | REST 每 3 分钟 + WS 实时 | 仅依赖 WS |
| 零持仓推送 | 推送 (重要修复) | 跳过 (历史 bug) |
| 拒单原因 | `setattr` 动态属性 | OrderData 增加 `rejected_reason` 字段 |

### 6.3 调试日志清理

文件中遗留了大量 `print(...)` 调试语句 (例如 `"hahaTrue"`, `"hahaFalse"`, `"hahaTrue<0"` 等),**生产环境建议替换为 loguru 结构化日志**或删除。这些 print 会在高频回报时刷屏。

---

## 7. 常见问题排查

| 现象 | 排查路径 |
|------|---------|
| WebSocket 频繁断连 | 检查代理稳定性 → ping/pong 日志 → `keep_user_stream` 失败计数 |
| 签名错误 `-1021` | 检查 `query_time()` 是否成功 → 本地时间是否同步 → secret 是否正确 |
| 订单回报重复 | 检查 `gateway.on_order` 的增量计算逻辑 (`traded < 0` 是否被丢弃) |
| 持仓与交易所不一致 | 检查零持仓推送分支 → 调用 `verify_portfolio_consistency()` |
| tick 队列积压 | 评估是否需要重新启用计数器节流 → 检查策略 `on_tick` 处理耗时 |
| 委托号冲突 | 检查 `connect_time` 计算 + `order_count_lock` 锁是否生效 |
| 行情断连后不恢复 | 检查 `on_connected` 中重新订阅逻辑 → `self.ticks` 是否被清空 |
| 拒单无原因 | 检查 `setattr(order, "rejected_reason", ...)` 是否被下游消费 |
| K 线下载死循环 | 检查 `len(data) < limit` 终止条件 + `start_time` 推进逻辑 |



| 文件 | 作用 |
|------|------|
| `binance_usdt_gateway.py` | 本文件 — 完整版 (REST + 行情 WS + 交易 WS) |
| `binance_market_only_gateway.py` | 仅行情订阅版本 (用于纯数据采集) |
| `binance_trade_only_gateway.py` | 仅交易版本 |
| `binance_return_only_gateway.py` | 仅回报订阅版本 |
| `binance_usdt_gateway_trader.py` | 交易专用版本 (实盘交易精简版) |
| `binance_usdt_gateway*.py` (历史备份) | 各时期演进快照,新功能开发请勿引用 |

> 同目录下大量 `binance_usdt_gateway*.py` (含日期、版本号、问题标注) 是历史快照,生产代码请始终使用 `binance_usdt_gateway.py`。
