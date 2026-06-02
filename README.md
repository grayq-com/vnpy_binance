# vnpy_binance

基于 [vnpy](https://github.com/vnpy/vnpy) 框架的币安 USDT 合约交易接口网关。

## 功能特性

- 支持币安 USDT 正向合约（全仓模式 / 单向持仓）
- REST API 下单、撤单、查询资金与持仓
- WebSocket 实时行情推送（Ticker / Depth / K线）
- WebSocket 交易推送（账户更新 / 委托回报 / 成交回报）
- 支持限价单（LIMIT）、市价单（MARKET）、止损单（STOP）、FAK、FOK
- 历史K线数据批量下载
- 支持实盘与模拟盘切换
- 支持代理连接

## 环境要求

- Python 3.10+
- vnpy
- vnpy_rest
- vnpy_websocket

## 快速使用

```python
from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from binance_usdt_gateway import BinanceUsdtGateway

event_engine = EventEngine()
main_engine = MainEngine(event_engine)
main_engine.add_gateway(BinanceUsdtGateway)

setting = {
    "key": "你的API_KEY",
    "secret": "你的API_SECRET",
    "服务器": "REAL",
    "代理地址": "",
    "代理端口": 0,
}
main_engine.connect(setting, "BINANCE")
```

## 免责声明

本项目仅供学习交流，数字货币交易存在高风险，使用本代码交易造成的盈亏概不负责。

## 联系方式

如有合作或定制开发需求，欢迎联系：jayhaha600@gmail.com
