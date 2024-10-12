from howtrader.trader.gateway import BaseGateway
from howtrader.trader.object import (
    AccountData,
    HistoryRequest,
    OrderData,
    TickData,
    BarData,
    ContractData,
    TradeData,
    OriginalKlineData,
)
from howtrader.trader.object import (
    OrderRequest,
    SubscribeRequest,
    UnsubcribeRequest,
    CancelRequest,
)
from howtrader.trader.constant import (
    Exchange,
    Product,
    Direction,
    OrderType,
    Status,
    Interval,
)
from typing import Any, Dict, List, Tuple, Optional
from python_trading.trading import core as contek_core
from python_trading.gateway.remote.gateway import RemoteGateway as contek_RemoteGateway
from python_trading.gateway.remote.gateway import GatewayConfig as contek_GatewayConfig
from python_trading.gateway.remote.gateway import PlaceOrderReq as contek_PlaceOrderReq
from python_trading.md.remote.client import Client as contek_Client
from python_trading.md.remote.client import ClientConfig as contek_ClientConfig
from python_trading.md.remote.client import MdSub as contek_MdSub
from copy import copy
from threading import Lock
from howtrader.event import EventEngine
from howtrader.trader.constant import LOCAL_TZ
from threading import Thread
from asyncio import (
    get_running_loop,
    new_event_loop,
    set_event_loop,
    run_coroutine_threadsafe,
    AbstractEventLoop,
    Future,
    set_event_loop_policy,
)
import result
from datetime import datetime
from decimal import Decimal
import aiohttp
import asyncio
from aiohttp import ClientSession, ClientResponse, ClientTimeout
import pandas as pd
import numpy as np
from json import loads
import json
import os

DIRECTION_CONTEK2VT = {
    contek_core.OrderSide.buy: Direction.LONG,
    contek_core.OrderSide.sell: Direction.SHORT,
}
DIRECTION_VT2CONTEK = {v: k for k, v in DIRECTION_CONTEK2VT.items()}
ORDERTYPE_CONTEK2VT = {
    contek_core.OrderType.limit: OrderType.LIMIT,
    contek_core.OrderType.market: OrderType.TAKER,
    contek_core.OrderType.stop_limit: OrderType.STOP,
    contek_core.OrderType.fok: OrderType.FOK,
}
ORDERTYPE_VT2CONTEK = {v: k for k, v in ORDERTYPE_CONTEK2VT.items()}
STATUS_CONTEK2VT = {
    # contek_core.OrderStatus.pending: Status.SUBMITTING,
    contek_core.OrderStatus.sent: Status.SUBMITTING,
    contek_core.OrderStatus.acked: Status.NOTTRADED,
    contek_core.OrderStatus.partial_filled: Status.PARTTRADED,
    contek_core.OrderStatus.filled: Status.ALLTRADED,
    contek_core.OrderStatus.cancelled: Status.CANCELLED,
    contek_core.OrderStatus.rejected: Status.REJECTED,
}
# data time frame map
INTERVAL_VT2BINANCES: Dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.MINUTE_3: "3m",
    Interval.MINUTE_5: "5m",
    Interval.MINUTE_15: "15",
    Interval.MINUTE_30: "30m",
    Interval.HOUR: "1h",
    Interval.HOUR_2: "2h",
    Interval.HOUR_4: "4h",
    Interval.HOUR_6: "6h",
    Interval.HOUR_8: "8h",
    Interval.HOUR_12: "12h",
    Interval.DAILY: "1d",
    Interval.DAILY_3: "3d",
    Interval.WEEKLY: "1w",
    Interval.MONTH: "1M",
}

# contract symbol map
symbol_contract_map: Dict[str, ContractData] = {}


class Request(object):
    def __init__(
        self,
        url: str,
        params: dict = None,
        callback=None,
    ):
        self.url = url
        self.params = params
        self.callback = callback
        self.response = None


class ContekGateway(BaseGateway):
    """
    VN Trader Gateway for Contek.
    """

    default_name: str = "CONTEK"

    exchanges: Exchange = [Exchange.CONTEK]

    def __init__(self, event_engine: EventEngine, gateway_name) -> None:
        """"""
        super().__init__(event_engine, gateway_name)

        self.orders: Dict[str, OrderData] = {}
        self.is_connected = False

    def connect(self, setting: dict) -> None:
        """"""
        # init rest client
        rest_cfg = contek_GatewayConfig(
            client_id=setting["client_id"],
            router_addr=setting["router_addr"],
            pub_addr=setting["pub_addr"],
            account=setting["account"],
            exchange=setting["exchange"],
            curve_server_key=setting["curve_server_key"],
            curve_secret_key=setting["curve_secret_key"],
            curve_public_key=setting["curve_public_key"],
        )

        self.rest_api = ContekRestApi(self, rest_cfg)
        self.rest_api.connect()

        # init ws client
        ws_cfg = contek_ClientConfig(
            xsub_addr=setting["xsub_addr"], sub_addr=setting["sub_addr"]
        )
        self.websock_api = ContekWebsocketApi(self, ws_cfg)
        self.websock_api.connect()

        # connect successful
        self.is_connected = True

    def subscribe(self, req: SubscribeRequest) -> None:
        self.websock_api.subscribe_data(req)

    def unsubscribe(self, req: UnsubcribeRequest) -> None:
        self.websock_api.unsubscribe_data(req)

    def send_order(self, req: OrderRequest) -> str:
        return self.rest_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        self.rest_api.remove_order(req)

    def query_account(self) -> None:
        """query account balance"""
        self.rest_api.query_account()

    def on_order(self, order: OrderData) -> None:
        last_order: OrderData = self.orders.get(order.orderid, None)
        if not last_order:
            self.orders[order.orderid] = order
            super().on_order(copy(order))

        else:
            traded: Decimal = order.traded - last_order.traded
            if traded < 0:  # filter the order is not in sequence
                return None

            if traded > 0:
                trade: TradeData = TradeData(
                    symbol=order.symbol,
                    exchange=order.exchange,
                    orderid=order.orderid,
                    direction=order.direction,
                    price=order.traded_price,
                    volume=traded,
                    datetime=order.update_time,
                    gateway_name=self.gateway_name,
                )
                super().on_trade(trade)

            if traded == 0 and order.status == last_order.status:
                return None

            self.orders[order.orderid] = order
            super().on_order(copy(order))

    def query_priceticker(self) -> None:
        return self.rest_api.query_priceticker()

    def query_latest_kline(self, req: HistoryRequest) -> None:
        self.rest_api.query_latest_kline(req)

    def close(self):
        """close gateway"""
        self.rest_api.stop()
        self.websock_api.stop()


class ContekRestApi(contek_RemoteGateway):

    def __init__(self, gateway: ContekGateway, cfg: contek_GatewayConfig):
        super().__init__(cfg)
        self.cfg: contek_GatewayConfig = cfg
        self.gateway: ContekGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self._active = False
        self._loop: Optional[AbstractEventLoop] = None
        self.session: Optional[ClientSession] = None  # timeout=ClientTimeout(total=5)
        self.order_count = 1_000_000
        self.order_count_lock: Lock = Lock()
        self.connect_time = 0

    def connect(self):
        self.connect_time = (
            int(datetime.now().strftime("%y%m%d%H%M%S")) * self.order_count
        )
        # start event loop
        self.start()
        self.gateway.write_log("start connecting rest api")
        self.query_contract()
        self.query_account()

    def start(self):
        # start event loop
        if self._active:
            return None

        self._active = True
        try:
            self._loop = get_running_loop()
        except RuntimeError:
            self._loop = new_event_loop()

        start_event_loop(self._loop)
        run_coroutine_threadsafe(self._run(), self._loop)
        self.connect_zmq(self._loop)
        run_coroutine_threadsafe(self.on_order_update(), self._loop)

    async def _run(self):
        """run event loop"""
        set_event_loop(self._loop)
        self._loop.run_forever()

    def stop(self):
        self._active = False
        if self._loop and self._loop.is_running():
            self._loop.stop()

    def _new_order_id(self) -> int:
        """generate customized order id"""
        with self.order_count_lock:
            self.order_count += 1
            return self.order_count

    async def _get_response(self, request: Request):
        try:
            if not self.session:
                self.session = ClientSession()

            cr: ClientResponse = await self.session.get(
                request.url, params=request.params
            )
            text: str = await cr.text()
            data = loads(text)
            status_code = cr.status
            if status_code // 100 == 2:
                if request.callback:
                    request.callback(data, request)
                else:
                    return data
            else:
                self.gateway.write_log("Error occurd")
        except Exception as e:
            self.gateway.write_log(f"Error occurd: {e}")

    async def _process_request(
        self, method: str, *args, callback=None, callback_args=None
    ):
        method = getattr(self, method)
        res: result.Result = await method(*args)

        if res.is_err():
            self.gateway.write_log(res.err_value)

        data = res.ok_value
        if callback:
            callback(data, callback_args=callback_args)

    async def _request(self, method: str, *args):
        method = getattr(self, method)
        res: result.Result = await method(*args)

        if res.is_err():
            pass
        return res

    async def on_order_update(self):
        async for order_update in self.sub_order_update():
            order: OrderData = OrderData(
                symbol=order_update.exch_symbol,
                exchange=Exchange.CONTEK,
                orderid=str(order_update.id),
                type=ORDERTYPE_CONTEK2VT[order_update.type],
                direction=DIRECTION_CONTEK2VT[order_update.side],
                price=order_update.price,
                volume=Decimal(str(order_update.qty)),
                traded=Decimal(str(order_update.acc_traded_qty)),
                traded_price=Decimal(str(order_update.last_price)),
                status=STATUS_CONTEK2VT[order_update.status],
                datetime=(
                    generate_datetime(order_update.exch_update_time)
                    if order_update.exch_update_time != -1
                    else datetime(year=2020, month=1, day=1)
                ),
                update_time=generate_datetime(order_update.local_update_time),
                gateway_name=self.gateway_name,
                rejected_reason=order_update.rejected_code,
            )
            self.gateway.on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        return self.orders.get(orderid, None)

    def query_account(self):
        method = "get_positions"
        callback = self.on_query_account

        if self._loop:
            run_coroutine_threadsafe(
                self._process_request(method, callback=callback), self._loop
            )

    def on_query_account(self, data, callback_args=None):
        for asset, balance in data.items():
            account = AccountData(
                accountid=asset,
                balance=balance,
                frozen=0,
                gateway_name=self.gateway_name,
            )
            self.gateway.on_account(account)

        # self.gateway.write_log("account balance query success")

    def send_order(self, req: OrderRequest) -> str:
        """send/place order"""
        orderid: str = self.connect_time + self._new_order_id()

        # create OrderData object
        order: OrderData = req.create_order_data(orderid, self.gateway_name)

        # extract order data
        exch = contek_core.Exchange.binance_futures
        inst_type = contek_core.InstrumentType.linear
        direction = DIRECTION_VT2CONTEK[order.direction]
        order_type = ORDERTYPE_VT2CONTEK[order.type]

        # send order to gateway
        if order_type == contek_core.OrderType.stop_limit:
            order_params = json.loads(os.environ[req.vt_symbol])
            if direction == contek_core.OrderSide.buy:
                stop_price = float(order_params["stop_buy_price"])
            elif direction == contek_core.OrderSide.sell:
                stop_price = float(order_params["stop_sell_price"])
            req = contek_PlaceOrderReq(
                exch=exch,
                inst_type=inst_type,
                sym=order.symbol,
                price=float(order.price),
                stop_price=stop_price,
                qty=float(order.volume),
                side=direction,
                type=order_type,
                order_id=orderid,
                reduce_only=False,
            )
        else:
            req = contek_PlaceOrderReq(
                exch=exch,
                inst_type=inst_type,
                sym=order.symbol,
                price=float(order.price),
                stop_price=0.0,
                qty=float(order.volume),
                side=direction,
                type=order_type,
                order_id=orderid,
                reduce_only=False,
            )
        method = "place_order"

        if self._loop:
            run_coroutine_threadsafe(
                self._process_request(method, req),
                self._loop,
            )
        return order.vt_orderid

    def remove_order(self, req: CancelRequest) -> None:
        """cancel order"""
        orderid = int(req.orderid)
        method = "cancel_order"
        if self._loop:
            run_coroutine_threadsafe(self._process_request(method, orderid), self._loop)

    def query_contract(self):
        method = "get_symbol_info"
        callback = self.on_query_contract

        if self._loop:
            run_coroutine_threadsafe(
                self._process_request(method, callback=callback), self._loop
            )

    def on_query_contract(self, data: dict, callback_args=None):
        """query contract callback"""
        for symbol, symbol_info in data.items():
            # delete suffix
            symbol = symbol.split("_")[0]
            if "USD" not in symbol or symbol == "USDCUSDT":
                continue
            base, quote = symbol.split("USD")

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=Exchange.CONTEK,
                name=base + "/USD" + quote,
                pricetick=Decimal(str(symbol_info.tick_size)),
                size=Decimal("1"),
                min_volume=Decimal(str(symbol_info.min_qty)),
                min_notional=Decimal(str(symbol_info.min_notional)),
                product=Product.FUTURES,
                net_position=True,
                history_data=True,
                gateway_name=self.gateway_name,
                stop_supported=True,
            )
            self.gateway.on_contract(contract)
            symbol_contract_map[contract.symbol] = contract

        self.gateway.write_log("query contract successfully")

    def query_priceticker(self):
        url = "https://fapi.binance.com/fapi/v2/ticker/price"
        request = Request(url)
        if self._loop:
            fut: Future = run_coroutine_threadsafe(
                self._get_response(request), self._loop
            )
        data = fut.result()
        return data

    def query_latest_kline(self, req: HistoryRequest) -> None:

        interval = INTERVAL_VT2BINANCES.get(req.interval, None)
        if not interval:
            print(f"unsupport interval: {req.interval}")
            return None

        params: dict = {
            "symbol": req.symbol,
            "interval": interval,
            "limit": req.limit,
            # "endTime": end_time * 1000  # convert the start time into milliseconds
        }
        url = "https://fapi.binance.com/fapi/v1/klines"
        callback = self.on_query_latest_kline
        request = Request(url, params, callback)

        if self._loop:
            run_coroutine_threadsafe(self._get_response(request), self._loop)

    def on_query_latest_kline(self, datas, request: Request):
        if len(datas) > 0:
            df = pd.DataFrame(
                datas,
                dtype=np.float64,
                columns=[
                    "open_time",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "close_time",
                    "turnover",
                    "a2",
                    "a3",
                    "a4",
                    "a5",
                ],
            )
            df = df[["open_time", "open", "high", "low", "close", "volume", "turnover"]]
            df.set_index("open_time", inplace=True)
            df.index = pd.to_datetime(
                df.index, unit="ms"
            )  # + pd.Timedelta(hours=8) # use the utc time.

            symbol = request.params.get("symbol", "")
            interval = Interval(request.params.get("interval"))
            kline_data = OriginalKlineData(
                symbol=symbol,
                exchange=Exchange.CONTEK,
                interval=interval,
                klines=datas,
                kline_df=df,
                gateway_name=self.gateway_name,
            )

            self.gateway.on_kline(kline_data)


class ContekWebsocketApi(contek_Client):
    """Contek Market Data/Trades Client"""

    def __init__(self, gateway: ContekGateway, cfg: contek_ClientConfig):
        super().__init__(cfg)
        self.cfg = cfg
        self.gateway: ContekGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self._active = False
        self._loop = Optional[AbstractEventLoop]

        self.ticks: Dict[str, TickData] = {}
        self.reqid: int = 0

    def connect(self):
        """start event loop"""
        if self._active:
            return None

        self._active = True

        try:
            self._loop = get_running_loop()
        except RuntimeError:
            self._loop = new_event_loop()

        start_event_loop(self._loop)
        run_coroutine_threadsafe(self._run(), self._loop)

    def stop(self):
        self._active = False
        if self._loop and self._loop.is_running():
            self._loop.stop()

    async def _run(self):
        """run event loop"""
        async for md_sub, msg in self.listen():
            if md_sub.md_type == contek_core.MdType.connection:
                continue
            self.on_packet(md_sub, msg)

    def subscribe_data(self, req: SubscribeRequest) -> None:
        if req.symbol in self.ticks:
            return

        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(f"symbol {req.symbol} not found in contract")
            return

        self.reqid += 1

        # init tick data
        tick: TickData = TickData(
            symbol=req.symbol,
            name=symbol_contract_map[req.symbol].name,
            exchange=Exchange.CONTEK,
            datetime=datetime.now(LOCAL_TZ),
            gateway_name=self.gateway_name,
        )
        self.ticks[req.symbol.lower()] = tick

        md_types = [
            contek_core.MdType.trade,
            # MdType.quote,
            # contek_core.MdType.depth5,
            # MdType.depth10,
            # MdType.depth20,
            # MdType.orderbook,
        ]

        for md_type in md_types:
            self.subscribe(
                contek_MdSub(
                    exchange=contek_core.Exchange.binance_futures,
                    ins_type=contek_core.InstrumentType.linear,
                    symbol=req.symbol,
                    md_type=md_type,
                )
            )

    def unsubscribe_data(self, req: UnsubcribeRequest):
        if req.symbol.lower() not in self.ticks:
            return

        if req.symbol not in symbol_contract_map:
            self.gateway.write_log(f"symbol not found: {req.symbol}")
            return

        self.reqid += 1
        self.ticks.pop(req.symbol.lower())

        md_types = [
            contek_core.MdType.trade,
            # MdType.quote,
            # contek_core.MdType.depth5,
            # MdType.depth10,
            # MdType.depth20,
            # MdType.orderbook,
        ]

        for md_type in md_types:
            self.unsubscribe(
                contek_MdSub(
                    exchange=contek_core.Exchange.binance_futures,
                    ins_type=contek_core.InstrumentType.linear,
                    symbol=req.symbol,
                    md_type=md_type,
                )
            )

    def on_packet(self, md_sub: contek_MdSub, msg: bytes):
        """process packet"""
        symbol = md_sub.symbol.lower()
        if symbol not in self.ticks:
            return
        if md_sub.md_type.name == contek_core.MdType.trade.name:
            self.on_trade(md_sub, msg)
        elif md_sub.md_type.name == contek_core.MdType.depth5.name:
            self.on_depth5(md_sub, msg)

    def on_trade(self, md_sub: contek_MdSub, msg: bytes):
        tick: TickData = self.ticks[md_sub.symbol.lower()]
        tick.volume = float(msg.md.qty)
        tick.turnover = float(msg.md.qty * msg.md.price)
        tick.last_price = float(msg.md.price)
        tick.datetime = generate_datetime(msg.md.exch_ns)
        tick.localtime = generate_datetime(msg.local_ns)
        self.gateway.on_tick(tick)

    def on_depth5(self, md_sub: contek_MdSub, msg: bytes):
        tick: TickData = self.ticks[md_sub.symbol.lower()]
        tick.datetime = generate_datetime(msg.md.exch_ns)
        tick.localtime = generate_datetime(msg.local_ns)

        # filled bids data
        bids = msg.md.bids
        for n in range(min(5, len(bids))):
            price = bids[n].price
            qty = bids[n].qty
            tick.__setattr__("bid_price_" + str(n + 1), float(price))
            tick.__setattr__("bid_volume_" + str(n + 1), float(qty))

        # filled asks data
        asks = msg.md.asks
        for n in range(min(5, len(asks))):
            price = asks[n].price
            qty = asks[n].qty
            tick.__setattr__("ask_price_" + str(n + 1), float(price))
            tick.__setattr__("ask_volume_" + str(n + 1), float(qty))

        last_price = Decimal(
            (
                tick.ask_price_1 * tick.ask_volume_1
                + tick.bid_price_1 * tick.bid_volume_1
            )
            / (tick.ask_volume_1 + tick.bid_volume_1)
        )
        precision = Decimal(10) ** Decimal(str(tick.ask_price_1)).as_tuple().exponent
        tick.last_price = float(last_price.quantize(precision))
        self.gateway.on_tick(tick)


def start_event_loop(loop: AbstractEventLoop) -> None:
    """start event loop"""
    # if the event loop is not running, then create the thread to run
    if not loop.is_running():
        thread = Thread(target=run_event_loop, args=(loop,))
        thread.daemon = True
        thread.start()


def run_event_loop(loop: AbstractEventLoop) -> None:
    """run event loop"""
    set_event_loop(loop)
    loop.run_forever()


def generate_datetime(timestamp: float) -> datetime:
    """generate time"""
    dt: datetime = datetime.fromtimestamp(timestamp / (10**9))
    # dt: datetime = LOCAL_TZ.localize(dt)
    return dt
