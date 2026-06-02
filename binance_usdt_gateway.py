"""
1. 只支持全仓模式
2. 只支持单向持仓模式
3. 只支持正向合约
"""
import urllib
import hashlib
import hmac
import time
import threading
from copy import copy
from datetime import datetime, timedelta
from enum import Enum
from threading import Lock
from typing import Any
import json
from decimal import Decimal
import pandas as pd
from asyncio import run_coroutine_threadsafe
from time import sleep
import urllib.parse

from numpy import format_float_positional

from requests.exceptions import SSLError
from types import coroutine
from vnpy.event import Event, EventEngine
from vnpy.trader.constant import (
    Direction,
    Exchange,
    Product,
    Status,
    OrderType,
    Interval
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    Offset,
    AccountData,
    ContractData,
    PositionData,
    BarData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    HistoryRequest
)
from vnpy.trader.event import EVENT_TIMER

from vnpy_rest import Request, RestClient, Response
from vnpy_websocket import WebsocketClient
from pytz import timezone


# 中国时区
CHINA_TZ: timezone = timezone("Asia/Shanghai")#Asia/Shanghai

# 实盘正向合约REST API地址
F_REST_HOST: str = "https://fapi.binance.com"
# 实盘正向合约Websocket API地址
F_WEBSOCKET_TRADE_HOST: str = "wss://fstream.binance.com/ws/"
F_WEBSOCKET_DATA_HOST: str = "wss://fstream.binance.com/stream"

# 模拟盘正向合约REST API地址
F_TESTNET_REST_HOST: str = "https://testnet.binancefuture.com"

# 模拟盘正向合约Websocket API地址
F_TESTNET_WEBSOCKET_TRADE_HOST: str = "wss://stream.binancefuture.com/ws/"
F_TESTNET_WEBSOCKET_DATA_HOST: str = "wss://stream.binancefuture.com/stream"

# 委托状态映射
STATUS_BINANCES2VT: dict[str, Status] = {
    "NEW": Status.NOTTRADED,
    "PARTIALLY_FILLED": Status.PARTTRADED,
    "FILLED": Status.ALLTRADED,
    "CANCELED": Status.CANCELLED,
    "REJECTED": Status.REJECTED,
    "EXPIRED": Status.CANCELLED
}

# 委托类型映射
ORDERTYPE_VT2BINANCES: dict[OrderType, tuple[str, str]] = {
    OrderType.LIMIT: ("LIMIT", "GTC"),
    OrderType.MARKET: ("MARKET", "GTC"),
    OrderType.FAK: ("LIMIT", "IOC"),
    OrderType.FOK: ("LIMIT", "FOK"),
    OrderType.STOP:("STOP","GTC")
}
ORDERTYPE_BINANCES2VT: dict[tuple[str, str], OrderType] = {v: k for k, v in ORDERTYPE_VT2BINANCES.items()}

# 买卖方向映射
DIRECTION_VT2BINANCES: dict[Direction, str] = {
    Direction.LONG: "BUY",
    Direction.SHORT: "SELL"
}
DIRECTION_BINANCES2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2BINANCES.items()}


# 数据频率映射
INTERVAL_VT2BINANCES: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.HOUR: "1h",
    Interval.DAILY: "1d",
}

# 时间间隔映射
TIMEDELTA_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
    Interval.DAILY: timedelta(days=1),
}

# Weboscket超时设置为24小时
WEBSOCKET_TIMEOUT = 24 * 60 * 60


# 合约数据全局缓存字典
symbol_contract_map: dict[str, ContractData] = {}

# 成交id
VTTRADE_ID = str

# 鉴权类型
class Security(Enum):
    NONE: int = 0
    SIGNED: int = 1
    API_KEY: int = 2


class BinanceUsdtGateway(BaseGateway):
    """
    vn.py用于对接币安正向合约的交易接口。
    """

    default_name: str = "BINANCE"

    default_setting: dict[str, Any] = {
        "key": "",
        "secret": "",
        "服务器": ["REAL", "TESTNET"],
        "代理地址": "",
        "代理端口": 0,
    }

    exchanges: Exchange = [Exchange.BINANCE]

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.trade_ws_api: BinanceUsdtTradeWebsocketApi = BinanceUsdtTradeWebsocketApi(self)
        self.market_ws_api: BinanceUsdtDataWebsocketApi = BinanceUsdtDataWebsocketApi(self)
        self.rest_api: BinanceUsdtRestApi = BinanceUsdtRestApi(self)
        self.timer_count = 0
        self.orders: dict[str, OrderData] = {}


    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        key: str = setting["key"]
        secret: str = setting["secret"]
        server: str = setting["服务器"]
        proxy_host: str = setting["代理地址"]
        proxy_port: int = setting["代理端口"]

        self.rest_api.connect(key, secret, server, proxy_host, proxy_port)

        self.market_ws_api.connect(proxy_host, proxy_port, server)
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

        self.rest_api.set_position_mode(False)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.market_ws_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.rest_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        pass

    def query_position(self) -> None:
        """查询持仓"""
        pass

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        """查询历史数据"""
        return self.rest_api.query_history(req)

    def close(self) -> None:
        """关闭连接"""
        self.rest_api.stop()
        self.trade_ws_api.stop()
        self.market_ws_api.stop()

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理，定时查询资金"""
        self.rest_api.keep_user_stream()
        self.timer_count += 1
        if self.timer_count >= 60*30:
            self.timer_count = 0
            self.rest_api.query_account()
            self.rest_api.query_position()

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        order.update_time = generate_datetime(time.time() * 1000)
        last_order: OrderData = self.get_order(order.orderid)

        if not last_order:
            self.orders[order.orderid] = order
            super().on_order(copy(order))
        else:
            traded: Decimal = order.traded - last_order.traded
            if traded < 0:
                return None
            if traded > 0:
                trade: TradeData = TradeData(
                    symbol=order.symbol,
                    exchange=order.exchange,
                    orderid=order.orderid,
                    tradeid=VTTRADE_ID,
                    direction=order.direction,
                    offset=order.offset,
                    price=order.price,
                    volume=traded,
                    datetime=order.update_time,
                    gateway_name=self.gateway_name,
                )
                super().on_trade(trade)
            if traded == 0 and order.status == last_order.status:
                return None

            self.orders[order.orderid] = order
            super().on_order(copy(order))

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)


class BinanceUsdtRestApi(RestClient):
    """币安正向合约的REST API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.trade_ws_api: BinanceUsdtTradeWebsocketApi = self.gateway.trade_ws_api

        self.key: str = ""
        self.secret: str = ""

        self.user_stream_key: str = ""
        self.keep_alive_count: int = 0

        self.recv_window: int = 5000
        self.time_offset: int = 0

        self.order_count: int = 1_000
        self.order_count_lock: Lock = Lock()
        self.connect_time: int = 0

        self.product_type_map = {
            "USDT": "USDT-FUTURES",
            "USDC": "USDC-FUTURES"
        }

        self.keep_alive_failed_count: int = 0

    def set_position_mode(self, dual: bool):
        """
        设置持仓模式
        dual: True=双向持仓, False=单向持仓
        """


        data = {"security": Security.SIGNED}
        params = {"dualSidePosition": "true" if dual else "false"}
        self.add_request(
            method="POST",
            path="/fapi/v1/positionSide/dual",
            callback=self.on_set_position_mode,
            on_failed=self.on_set_position_mode_failed,
            data=data,
            params=params,
            extra=dual
        )


    def on_set_position_mode(self, data, request: Request):
        self.gateway.write_log(str(f"持仓模式设置成功: {'双向' if request.extra else '单向'}"))



    def on_set_position_mode_failed(self, status_code: int, request: Request):
        # 默认错误详情
        error_msg = ""
        if hasattr(request, "response") and request.response is not None:
            error_msg = request.response.text
            # 尝试解析币安返回的 JSON
            try:
                error_data = json.loads(error_msg)
                if error_data.get("code") == -4059:
                    # 视为正常
                    self.gateway.write_log("持仓模式已是目标状态，无需更改。")
                    return
            except Exception:
                pass

        # 其他情况按原逻辑处理
        self.gateway.write_log(str(f"持仓模式设置失败: {str(status_code)}"))
        if error_msg:
            self.gateway.write_log(str(f"错误详情: {str(error_msg)}"))

    def sign(self, request: Request) -> Request:
        """生成币安签名"""
        security: Security = request.data["security"]
        if security == Security.NONE:
            request.data = None
            return request

        if request.params:
            path: str = request.path + "?" + urllib.parse.urlencode(request.params)
        else:
            request.params = dict()
            path: str = request.path

        if security == Security.SIGNED:
            timestamp: int = int(time.time() * 1000)

            if self.time_offset > 0:
                timestamp -= abs(self.time_offset)
            elif self.time_offset < 0:
                timestamp += abs(self.time_offset)

            request.params["timestamp"] = timestamp

            query: str = urllib.parse.urlencode(sorted(request.params.items()))
            self.secret=bytes(self.secret)
            signature: bytes = hmac.new(self.secret,
                                        query.encode("utf-8"),
                                        digestmod=hashlib.sha256).hexdigest()
            sign=hmac.new(self.secret,
                                        query.encode("utf-8"),
                                        hashlib.sha256).hexdigest()

            query += f"&signature={signature}"
            path: str = request.path + "?" + query

        request.path = path
        request.params = {}
        request.data = {}

        # 添加请求头
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json",
            "X-MBX-APIKEY": self.key,
            "Connection": "close"
        }

        if security in [Security.SIGNED, Security.API_KEY]:
            request.headers = headers

        return request

    def connect(
        self,
        key: str,
        secret: str,
        server: str,
        proxy_host: str,
        proxy_port: int
    ) -> None:
        """连接REST服务器"""
        self.key = key
        self.secret = secret.encode()
        self.proxy_port = proxy_port
        self.proxy_host = proxy_host
        self.server = server

        self.connect_time = (
            int(datetime.now().strftime("%m%d%H%M")) * self.order_count
        )

        if self.server == "REAL":
            self.init(F_REST_HOST, proxy_host, proxy_port)
        else:
            self.init(F_TESTNET_REST_HOST, proxy_host, proxy_port)
        self.start()

        self.gateway.write_log("BINANCE REST API启动成功")

        self.query_time()
        self.query_contract()
        self.query_account()
        self.query_position()
        self.query_order()
        self.start_user_stream()


    def set_leverage(self, symbol: str):
        """
        设置杠杆
        """
        data = {"security": Security}
        params={"symbol":symbol,"leverage": 1}
        self.add_request(
            method="POST",
            path="/fapi/v1/leverage",
            callback=self.on_leverage,
            on_failed=self.on_leverage_failed,
            data=data,
            params=params,
            extra=params
        )
    # ----------------------------------------------------------------------------------------------------
    def on_leverage(self, data, request: Request):
        """收到杠杆数据回调"""
        pass

    def on_leverage_failed(self,status_code: int, request: Request):
        """收到杠杆数据回调"""
        self.failed_with_timestamp(request)
        #self.set_leverage()

    def query_time(self) -> None:
        """查询时间"""
        data: dict = {
            "security": Security.NONE
        }

        path: str = "/fapi/v1/time"

        return self.add_request(
            "GET",
            path,
            callback=self.on_query_time,
            data=data
        )

    def query_account(self) -> None:
        """查询资金"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v2/account"#balance

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_account,
            data=data
        )

    def query_position(self) -> None:
        """查询持仓"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v2/positionRisk"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_position,
            data=data
        )

    def query_order(self) -> None:
        """查询未成交委托"""
        data: dict = {"security": Security.SIGNED}

        path: str = "/fapi/v1/openOrders"
        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_order,
            data=data
        )

    def query_contract(self) -> None:
        """查询合约信息"""
        data: dict = {
            "security": Security.NONE
        }

        path: str = "/fapi/v1/exchangeInfo"

        self.add_request(
            method="GET",
            path=path,
            callback=self.on_query_contract,
            data=data
        )

    def _new_order_id(self) -> int:
        """生成本地委托号"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        orderid: str = str(self.connect_time + self._new_order_id())

        order: OrderData = req.create_order_data(
            orderid,
            self.gateway_name
        )
        self.gateway.on_order(order)

        data: dict = {
            "security": Security.SIGNED
        }

        # 生成委托请求
        params: dict = {
            "symbol": req.symbol,
            "side": DIRECTION_VT2BINANCES[req.direction],
            "quantity": format_float(req.volume),
            "newClientOrderId": orderid,
        }

        if req.type == OrderType.MARKET:
            params["type"] = "MARKET"
        elif req.type == OrderType.STOP:
            params["type"] = "STOP"
            params["price"] = format_float(req.price)
            params["stopPrice"] = format_float(req.price)
        else:
            order_type, time_condition = ORDERTYPE_VT2BINANCES[req.type]
            params["type"] = order_type
            params["timeInForce"] = time_condition
            params["price"] = format_float(req.price)

        if req.offset == Offset.CLOSE:
            params['reduceOnly'] = True

        path: str = "/fapi/v1/order"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_send_order,
            data=data,
            params=params,
            extra=order,
            on_error=self.on_send_order_error,
            on_failed=self.on_send_order_failed
        )

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        data: dict = {
            "security": Security.SIGNED
        }

        params: dict = {
            "symbol": req.symbol,
            "origClientOrderId": req.orderid
        }

        path: str = "/fapi/v1/order"

        order: OrderData = self.gateway.get_order(req.orderid)
        self.add_request(
            method="DELETE",
            path=path,
            callback=self.on_cancel_order,
            params=params,
            data=data,
            on_failed=self.on_cancel_failed,
            extra=order
        )

    def start_user_stream(self) -> Request:
        """生成listenKey"""
        data: dict = {
            "security": Security.API_KEY
        }

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="POST",
            path=path,
            callback=self.on_start_user_stream,
            on_failed=self.on_start_user_stream_failed,
            on_error=self.on_start_user_stream_error,
            data=data
        )



    def keep_user_stream(self) -> Request:
        """延长listenKey有效期"""
        self.keep_alive_count += 1
        if self.keep_alive_count < 600:
            return None
        self.keep_alive_count = 0

        data: dict = {
            "security": Security.API_KEY
        }

        params: dict = {
            "listenKey": self.user_stream_key
        }

        path: str = "/fapi/v1/listenKey"

        self.add_request(
            method="PUT",
            path=path,
            callback=self.on_keep_user_stream,
            params=params,
            data=data,
            on_failed=self.on_keep_user_stream_failed,
            on_error=self.on_keep_user_stream_error
        )
    def on_keep_user_stream_failed(self, status_code: int, request: Request):
        self.failed_with_timestamp(request)
        self.keep_alive_failed_count += 1
        if self.keep_alive_failed_count <= 3:
            self.keep_alive_count = 1200000
            self.keep_user_stream()
        else:
            self.keep_alive_failed_count = 0
            self.start_user_stream()

    def on_query_time(self, data: dict, request: Request) -> None:
        """时间查询回报"""
        local_time: int = int(time.time() * 1000)
        server_time: int = int(data["serverTime"])
        self.time_offset: int = local_time - server_time

    def on_query_time_failed(self, status_code: int, request: Request):
        self.query_time()

    def on_query_time_error(self,  exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        self.query_time()

    def on_query_account(self, data: dict, request: Request) -> None:
        """资金查询回报"""
        for asset in data["assets"]:
            account: AccountData = AccountData(
                accountid=asset["asset"],
                balance=float(asset["walletBalance"]),
                frozen=float(asset["maintMargin"]),
                gateway_name=self.gateway_name,

            )

            if account.balance:
                self.gateway.on_account(account)

        self.gateway.write_log("BINANCE账户资金查询成功")

    def on_query_position(self, data: dict, request: Request) -> None:
        """持仓查询回报"""
        for d in data:
            v=float(d["positionAmt"])
            if v > 0:
                direction = Direction.LONG
                
            elif v < 0:
                direction = Direction.SHORT

            else:
                continue  # 无持仓
            position: PositionData = PositionData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                direction=direction,
                volume=float(d["positionAmt"]),
                price=float(d["entryPrice"]),
                pnl=float(d["unRealizedProfit"]),
                gateway_name=self.gateway_name,
            )
            if position.volume:
                volume = d["positionAmt"]
                if '.' in volume:
                    position.volume = float(d["positionAmt"])
                else:
                    position.volume = int(d["positionAmt"])

                self.gateway.on_position(position)

        self.gateway.write_log("BINANCE持仓信息查询成功")

    def on_query_order(self, datas: list, request: Request) -> None:
        """未成交委托查询回报"""
        for data in datas:
            key: tuple[str, str] = (data["type"], data["timeInForce"])
            order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)
            if not order_type:
                continue
            traded = Decimal(data.get("executedQty", "0"))
            price = Decimal(data["price"])
            order: OrderData = OrderData(
                orderid=data["clientOrderId"],
                symbol=data["symbol"],
                exchange=Exchange.BINANCE,
                price=price,
                volume=Decimal(data["origQty"]),
                traded=traded,
                type=order_type,
                direction=DIRECTION_BINANCES2VT[data["side"]],
                status=STATUS_BINANCES2VT.get(data["status"], None),
                datetime=generate_datetime(data.get("time")),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

        self.gateway.write_log("BINANCE委托信息查询成功")

    def on_query_contract(self, data: dict, request: Request) -> None:
        """合约信息查询回报"""
        for d in data["symbols"]:
            base_currency: str = d["baseAsset"]
            quote_currency: str = d["quoteAsset"]
            name: str = f"{base_currency.upper()}/{quote_currency.upper()}"

            pricetick: Decimal = Decimal("1")
            min_volume: Decimal = Decimal("1")


            for f in d["filters"]:
                if f["filterType"] == "PRICE_FILTER":
                    tick = str(f["tickSize"]).rstrip("0")
                    pricetick = Decimal(tick)
                elif f["filterType"] == "LOT_SIZE":
                    step = str(f["stepSize"]).rstrip("0")
                    min_volume = Decimal(step)


            contract: ContractData = ContractData(
                symbol=d["symbol"],
                exchange=Exchange.BINANCE,
                name=name,
                pricetick=pricetick,
                size=Decimal("1"),
                min_volume=min_volume,
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True
            )
            self.gateway.on_contract(contract)

            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log("BINANCE合约信息查询成功")

    def on_send_order(self, data: dict, request: Request) -> None:
        """委托下单回报"""
        if request.extra:
            order: OrderData = copy(request.extra)
            traded = Decimal(data.get('executedQty', '0'))
            price = Decimal(data["price"])

            order.traded = traded
            order.price = price
            order.status = STATUS_BINANCES2VT.get(data.get('status'), Status.NOTTRADED)
            self.gateway.on_order(order)



    def on_send_order_failed(self, status_code: str, request: Request) -> None:
        """委托下单失败服务器报错回报"""
        self.failed_with_timestamp(request)

        # 解析响应内容
        try:
            response_data = json.loads(request.response.text)
            code = response_data.get("code")
            msg = response_data.get("msg", "未知错误")
        except Exception as e:
            code = None
            msg = f"无法解析响应：{request.response.text}"

        # 构造清晰的日志消息
        log_msg = f"{self.gateway}委托失败，状态码：{status_code}，错误码：{code}，错误信息：{msg}"
        self.gateway.write_log(log_msg)

        # 更新订单状态
        if request.extra:
            order: OrderData = copy(request.extra)
            order.status = Status.REJECTED
            order.rejected_reason = f"API错误: {code}, {msg}"
            self.gateway.on_order(order)

    def on_send_order_error(self, exception_type: type, exception_value: Exception, tb, request: Request) -> None:
        """委托下单回报函数报错回报"""
        if request.extra:
            order: OrderData = copy(request.extra)
            order.status = Status.REJECTED
            order.rejected_reason = "on_send_order_error"
            self.gateway.on_order(order)

        # 只在非连接/SSL错误时打印详细错误
        if not issubclass(exception_type, (ConnectionError, SSLError)):
            error_msg = f"{exception_type.__name__}: {exception_value}"
            self.gateway.write_log(f"{self.gateway}委托失败，异常类型：{error_msg}")

    def on_cancel_order(self, data: dict, request: Request) -> None:
        if request.extra:
            order: OrderData = copy(request.extra)
            traded = Decimal(data.get("executedQty", "0"))
            price = Decimal(data['price'])

            order.traded = traded
            order.price = price
            order.status = STATUS_BINANCES2VT.get(data.get('status'), Status.CANCELLED)
            self.gateway.on_order(order)
        #如果没有撤销成功（request.extra=0），就继续获得orderdata，然后继续记录在orders上，发送delete撤销
        else:
            key: tuple[str, str] = (data.get("type"), data.get("timeInForce"))
            order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, OrderType.LIMIT)

            traded = Decimal(data.get("executedQty", "0"))

            price = Decimal(data["price"])

            order: OrderData = OrderData(
                orderid=data.get("clientOrderId"),
                symbol=data.get("symbol"),
                exchange=Exchange.BINANCE,
                price=price,
                volume=Decimal(data.get("origQty")),
                traded=traded,
                type=order_type,
                direction=DIRECTION_BINANCES2VT.get(data.get("side")),
                status=STATUS_BINANCES2VT.get(data.get("status"), Status.CANCELLED),
                datetime=generate_datetime(float(data.get("updateTime", time.time() * 1000))),
                gateway_name=self.gateway_name,
            )
            self.gateway.on_order(order)

    def on_cancel_failed(self, status_code: str, request: Request) -> None:
        """撤单回报函数报错回报"""
        self.failed_with_timestamp(request)
        if request.extra:
            order: OrderData = copy(request.extra)
            orderid = order.orderid
            order.status = Status.REJECTED
            self.gateway.on_order(order)


        msg = f"撤单失败，状态码：{status_code}，信息：{request.response.text}"
        self.gateway.write_log(str(msg))

    def on_start_user_stream(self, data: dict, request: Request) -> None:
        """生成listenKey回报"""
        self.user_stream_key = data["listenKey"]
        self.keep_alive_count = 0


        if self.server == "REAL":
            url = F_WEBSOCKET_TRADE_HOST + self.user_stream_key
        else:
            url = F_TESTNET_WEBSOCKET_TRADE_HOST + self.user_stream_key

        self.trade_ws_api.connect(url, self.proxy_host, self.proxy_port)

    def on_start_user_stream_failed(self, status_code: int, request: Request):
        self.failed_with_timestamp(request)
        self.start_user_stream()

    def on_start_user_stream_error(self, exception_type: type, exception_value: Exception, tb, request: Request):
        self.start_user_stream()

    def on_keep_user_stream(self, data: dict, request: Request) -> None:
        """延长listenKey有效期回报"""
        pass

    def on_keep_user_stream_error(
        self, exception_type: type, exception_value: Exception, tb, request: Request
    ) -> None:
        """延长listenKey有效期函数报错回报"""
        # 当延长listenKey有效期时，忽略超时报错
        if not issubclass(exception_type, TimeoutError):
            self.on_error(exception_type, exception_value, tb, request)

    def query_history(self, req: HistoryRequest) -> list[BarData]:
        #查询历史数据
        history: list[BarData] = []
        limit: int = 1500

        start_time: int = int(datetime.timestamp(req.start))

        while True:
            # 创建查询参数
            params: dict = {
                "pair": req.symbol,
                "interval": INTERVAL_VT2BINANCES[req.interval],
                "limit": limit
            }
            params["contractType"]="PERPETUAL"
            params["startTime"] = start_time * 1000

            path: str = "/fapi/v1/continuousKlines"
            if req.end:
                end_time = int(datetime.timestamp(req.end))
                params["endTime"] = end_time * 1000     # 转换成毫秒

            resp: Response = self.request(
                "GET",
                path=path,
                data={"security": Security.NONE},
                params=params
            )

            # 如果请求失败则终止循环
            if resp.status_code // 100 != 2:
                msg: str = f"获取历史数据失败，状态码：{resp.status_code}，信息：{resp.text}"
                self.gateway.write_log(str(msg))
                break
            else:
                data: dict = resp.json()
                if not data:
                    msg: str = f"获取历史数据为空，开始时间：{start_time}"
                    self.gateway.write_log(str(msg))
                    break

                buf: list[BarData] = []

                for row in data:
                    bar: BarData = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=generate_datetime(row[0]),
                        interval=req.interval,
                        volume=float(row[5]),
                        turnover=float(row[7]),
                        open_price=float(row[1]),
                        high_price=float(row[2]),
                        low_price=float(row[3]),
                        close_price=float(row[4]),
                        gateway_name=self.gateway_name
                    )
                    buf.append(bar)

                begin: datetime = buf[0].datetime
                end: datetime = buf[-1].datetime

                history.extend(buf)
                msg: str = f"获取历史数据成功，{req.symbol} - {req.interval.value}，{begin} - {end}"
                self.gateway.write_log(str(msg))

                #如果下载数据小于限制数1500，就跳出
                if len(data) < limit:
                    break
                #在上面，我们定义了startime，api就确定好了endtime，所以到了这里就会结束
                # 如果下载数据大于限制数1500，就把目前下载的数据的时间+时间间隔，然后就到sleep（0.5）停止0.5秒后，继续下载（因为前面是while true
                # ，促发了死循环）
                start_dt = bar.datetime + TIMEDELTA_MAP[req.interval]
                start_time = int(datetime.timestamp(start_dt))

            sleep(0.5)


        return history


    def failed_with_timestamp(self, request: Request):
        try:
            if request and request.response and request.response.text:
                resp = json.loads(request.response.text)
                if resp.get('code') == -1021:
                    self.query_time()
        except Exception:
            pass

class BinanceUsdtTradeWebsocketApi(WebsocketClient):
    """币安正向合约的交易Websocket API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

    def connect(self, url: str, proxy_host: str, proxy_port: int) -> None:
        self.init(url, proxy_host, proxy_port, ping_interval=25, receive_timeout=40)  # 设置 ping 间隔为 25 秒
        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("BINANCE 交易Websocket API连接成功")



    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""

        if "ping" in packet:
            self.send_packet({"pong": packet["ping"]})
            return

        if packet["e"] == "ACCOUNT_UPDATE":
            self.on_account(packet)
        elif packet["e"] == "ORDER_TRADE_UPDATE":
            self.on_order(packet)
        elif packet["e"] == "listenKeyExpired":
            self.on_listen_key_expired()

    def on_listen_key_expired(self) -> None:
        """ListenKey过期"""
        self.gateway.write_log("listenKey过期")
        self.disconnect()

    def disconnect(self) -> None:
        """主动断开websocket链接"""
        self._active = False
        ws = self._ws
        if ws:
            coro = ws.close()
            run_coroutine_threadsafe(coro, self._loop)

    def on_account(self, packet: dict) -> None:
        """资金更新推送"""
        for acc_data in packet["a"]["B"]:
            account: AccountData = AccountData(
                accountid=acc_data["a"],
                balance=float(acc_data["wb"]),
                frozen=float(acc_data["wb"]) - float(acc_data["cw"]),
                gateway_name=self.gateway_name
            )

            if account.balance:
                self.gateway.on_account(account)

        for pos_data in packet["a"]["P"]:
            if pos_data["ps"] == "BOTH":
                volume = pos_data["pa"]
                if '.' in volume:
                    volume = float(volume)
                else:
                    volume = int(volume)

                
                if volume > 0:
                    direction = Direction.LONG
                    
                elif volume < 0:
                    direction = Direction.SHORT

                else:
                    continue  # 无持仓

                position: PositionData = PositionData(
                    symbol=pos_data["s"],
                    exchange=Exchange.BINANCE,
                    direction=direction,
                    volume=volume,
                    price=float(pos_data["ep"]),
                    pnl=float(pos_data["cr"]),
                    gateway_name=self.gateway_name,
                )
                self.gateway.on_position(position)

    def on_order(self, packet: dict) -> None:
        """委托更新推送"""
        ord_data: dict = packet["o"]
        key: tuple[str, str] = (ord_data["o"], ord_data["f"])
        order_type: OrderType = ORDERTYPE_BINANCES2VT.get(key, None)

        price = Decimal(ord_data["p"])
        if price <= 0:
            price = Decimal(ord_data['ap'])


        offset = self.gateway.get_order(ord_data["c"]).offset if self.gateway.get_order(ord_data["c"]) else None
        order: OrderData = OrderData(
            symbol=ord_data["s"],
            exchange=Exchange.BINANCE,
            orderid=str(ord_data["c"]),
            type=order_type,
            direction=DIRECTION_BINANCES2VT[ord_data["S"]],
            price=price,
            volume=Decimal(ord_data["q"]),
            traded=Decimal(ord_data["z"]),
            status=STATUS_BINANCES2VT[ord_data["X"]],
            datetime=generate_datetime(packet["E"]),
            gateway_name=self.gateway_name,
            offset=offset
        )
        global VTTRADE_ID
        VTTRADE_ID =ord_data["t"]

        self.gateway.on_order(order)



    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("BINANCE 交易Websocket API断开")
        self.gateway.rest_api.start_user_stream()


class BinanceUsdtDataWebsocketApi(WebsocketClient):
    """币安正向合约的行情Websocket API"""

    def __init__(self, gateway: BinanceUsdtGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: BinanceUsdtGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.ticks: dict[str, TickData] = {}
        self.reqid: int = 0


    def connect(
        self,
        proxy_host: str,
        proxy_port: int,
        server: str
    ) -> None:
        """连接Websocket行情频道"""
        if server == "REAL":
            self.init(F_WEBSOCKET_DATA_HOST, proxy_host, proxy_port, ping_interval=25, receive_timeout=40)
        else:
            self.init(F_TESTNET_WEBSOCKET_DATA_HOST, proxy_host, proxy_port, ping_interval=25, receive_timeout=40)
        self.start()

    def on_connected(self) -> None:
        """连接成功回报"""
        self.gateway.write_log("BINANCE 行情Websocket API连接成功")

        # 重新订阅行情
        if self.ticks:
            channels = []
            for symbol in self.ticks.keys():
                channels.append(f"{symbol}_perpetual@continuousKline_1m")
                channels.append(f"{symbol}@ticker")
                channels.append(f"{symbol}@depth5")

            req: dict = {
                "method": "SUBSCRIBE",
                "params": channels,
                "id": self.reqid
            }
            self.send_packet(req)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if req.symbol in self.ticks:
            return

        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(str(f"找不到该合约代码{req.symbol}"))
            return

        self.reqid += 1
        tick: TickData = TickData(
            symbol=req.symbol,
            name=symbol_contract_map[req.symbol].name,
            exchange=Exchange.BINANCE,
            datetime=datetime.now(CHINA_TZ),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol.lower()] = tick

        channels = [
            f"{req.symbol.lower()}_perpetual@continuousKline_1m",
            f"{req.symbol.lower()}@ticker",
            f"{req.symbol.lower()}@depth5"
        ]

        req: dict = {
            "method": "SUBSCRIBE",
            "params": channels,
            "id": self.reqid
        }
        self.send_packet(req)

    def on_packet(self, packet: dict) -> None:
        """推送数据回报"""
        # 处理 ping 消息
        if "ping" in packet:
            self.send_packet({"pong": packet["ping"]})
            return

        stream: str = packet.get("stream", None)
        if not stream:
            return

        threading.Thread(target=self.process_tick_data, args=(packet,)).start()


    def process_tick_data(self, packet: dict) -> None:
        """处理接收到的 tick 数据"""

        data: dict = packet["data"]
        symbol, channel = packet["stream"].split("@")
        symbol = symbol.split("_")[0].lower()

        if symbol not in self.ticks:
            return

        tick: TickData = self.ticks[symbol]

        if channel == "ticker":
            tick.cKline_1m = False
            tick.volume = float(data['v'])
            tick.turnover = float(data['q'])
            tick.open_price = float(data['o'])
            tick.high_price = float(data['h'])
            tick.low_price = float(data['l'])
            tick.last_price = float(data['c'])
            tick.datetime = generate_datetime(float(data['E']))
        elif channel == "depth5":
            # 处理深度数据
            tick.cKline_1m = False
            bids: list = data["b"]
            for n in range(min(5, len(bids))):
                price, volume = bids[n]
                tick.__setattr__("bid_price_" + str(n + 1), float(price))
                tick.__setattr__("bid_volume_" + str(n + 1), float(volume))

            asks: list = data["a"]
            for n in range(min(5, len(asks))):
                price, volume = asks[n]
                tick.__setattr__("ask_price_" + str(n + 1), float(price))
                tick.__setattr__("ask_volume_" + str(n + 1), float(volume))
        else:
            tick.cKline_open = float(data['k']['o'])
            tick.cKline_high = float(data['k']['h'])
            tick.cKline_low =float( data['k']['l'])
            tick.cKline_close = float(data['k']['c'])
            tick.cKline_volume = float(data['k']['v'])
            tick.cKline_turnover = float(data['k']['V'])
            tick.cKline_1m =  bool(data['k']['x'])
            tick.cKdatetime = generate_datetime(float(data['k']['t']))

        if tick.last_price:
            tick.localtime = datetime.now()
            self.gateway.on_tick(copy(tick))

    def on_disconnected(self) -> None:
        """连接断开回报"""
        self.gateway.write_log("BINANCE 行情Websocket API断开")



def generate_datetime(timestamp: float) -> datetime:
    """生成时间"""
    dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    dt_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    dt_final = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
    return dt_final


def format_float(f: float) -> str:
    """
    Convert float number to string with correct precision.

    Fix potential error -1111: Parameter 'quantity' has too much precision
    """
    return format_float_positional(f, trim='-')

