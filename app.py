from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import threading

from orderbook import OrderBook

app = Flask(__name__)

exchange = {}

recipient_wallet_id_map = {}


@app.route("/", methods=["GET"])
def home():
    return "PAPLE Mini Exchange"


@app.route("/order_book", methods=["GET"])
def get_order_book():
    exchange_key = "{}-to-{}".format(request.args.get("source_currency"), request.args.get("target_currency"))
    order_book = exchange.get(exchange_key)

    if order_book is None:
        return jsonify({"msg": "Order book not found"}), 404

    return jsonify(format_order_book(order_book))


@app.route("/liquidity", methods=["POST"])
def add_liquidity():
    request_data = request.get_json()
    order = {
        "type": "limit",
        "side": "bid",
        "quantity": request_data["quantity"],
        "price": request_data["price"],
        "trade_id": request_data["trader_id"]
    }

    exchange_key = "{}-to-{}".format(request_data["source_currency"], request_data["target_currency"])
    order_book = exchange.get(exchange_key)
    if order_book is None:
        order_book = OrderBook()
        exchange[exchange_key] = order_book

    trades, _ = order_book.process_order(order, False, False)

    payments = format_trades(trades, request_data)[0]

    # asynchronously submit payments
    if len(payments) > 0:
        thread = threading.Thread(target=make_payments, args=payments)
        thread.start()

    return jsonify({
        "order_book": format_order_book(order_book),
        "payments": payments
    }), 201


@app.route("/market_order", methods=['POST'])
def market_order():
    request_data = request.get_json()
    order = {
        "type": "market",
        "side": "ask",
        "quantity": request_data["quantity"],
        "trade_id": request_data["sender_wallet_id"]
    }

    exchange_key = "{}-to-{}".format(request_data["target_currency"], request_data["source_currency"])
    order_book = exchange.get(exchange_key)
    if order_book is None:
        return jsonify({"msg": "Order book not found"}), 404

    trades, order_id = order_book.process_order(order, False, False)
    payments, remaining_quantity = format_trades(trades, request_data)

    if remaining_quantity > 0:
        if len(trades) != 0:
            # place limit ask order
            not_so_dummy_ask_offer = round(trades[-1]["price"], 4)
            ask_order = {
                "type": "limit",
                "side": "ask",
                "quantity": remaining_quantity,
                "price": not_so_dummy_ask_offer,
                "trade_id": request_data["sender_wallet_id"]
            }
            _, new_order = order_book.process_order(ask_order, False, False)
            recipient_wallet_id_map[new_order["order_id"]] = request_data["recipient_wallet_id"]

            return jsonify({
                "payments": payments,
                "msg": "Not enough liquidity to fulfil full order. A standing order for the remaining {}{} at a "
                       "price of {} has been placed. It will automatically execute once liquidity is available "
                       "".format(request_data["source_currency"], remaining_quantity, not_so_dummy_ask_offer)
            })
        else:
            return jsonify({
                "payments": payments,
                "msg": "Not enough liquidity to fulfil full order. Also couldn't determine price to place ask offer, "
                       "hence no ask order is placed"
            })

    # asynchronously submit payments
    if len(payments) > 0:
        thread = threading.Thread(target=make_payments, args=payments)
        thread.start()

    return jsonify({
        "payments": payments,
        "msg": "Order Fully Executed"
    })


def make_payments(payments):
    url = "http://paple.westeurope.cloudapp.azure.com:10050/paple/transfer/payments"

    data = []
    for payment in payments:
        sender = payment["sender"]
        if sender[:3] == "LP_":
            sender = "{}_{}".format(sender, payment["currency"])

        recipient = payment["recipient"]
        if recipient[:3] == "LP_":
            recipient = "{}_{}".format(recipient, payment["currency"])

        data.append({
            "senderWalletId": sender,
            "recipientWalletId": recipient,
            "amount": payment["quantity"]*100,
            "currencyCode": payment["currency"],
            "type": "REGULAR",
            "anonymous": True
        })

    resp = requests.post(url, data={"payments": data})
    print(resp.status_code, resp.reason, resp.content)


def format_order(order):
    return {
        "order_id": str(order.order_id),
        "quantity": str(order.quantity),
        "price": str(round(order.price, 4)),
        "trader_id": str(order.trade_id)
    }


def format_order_book(order_book):
    bid_price_map = {}
    for price, order_list in order_book.bids.price_map.items():
        temp_order_list = {}
        for order in order_list:
            temp_order_list[order.order_id] = format_order(order)

        bid_price_map[str(round(price, 4))] = temp_order_list

    ask_price_map = {}
    for price, order_list in order_book.asks.price_map.items():
        temp_order_list = {}
        for order in order_list:
            temp_order_list[order.order_id] = format_order(order)

        ask_price_map[str(round(price, 4))] = temp_order_list

    return {
        "bids": bid_price_map,
        "asks": ask_price_map
    }


def format_trades(trades, request_data):
    remaining_order = request_data["quantity"]
    payments = []

    for trade in trades:
        remaining_order -= trade["quantity"]

        # "recipient": request_data.get("recipient_wallet_id") or recipient_wallet_id_map[trade["party1"][2]]
        # if ultimate_recipient_id is None: this must have emanated from an ask
        ultimate_recipient_id = request_data.get("recipient_wallet_id")
        if ultimate_recipient_id is None:
            payments.append({
                "sender": trade["party2"][0],
                "recipient": recipient_wallet_id_map[trade["party1"][2]],
                "currency": request_data["source_currency"],
                "quantity": round(float(trade["quantity"]) * float(trade["price"]), 2)
            })
            payments.append({
                "sender": trade["party1"][0],
                "recipient": trade["party2"][0],
                "currency": request_data["target_currency"],
                "quantity": float(trade["quantity"])
            })
        else:
            payments.append({
                "sender": trade["party2"][0],
                "recipient": trade["party1"][0],
                "currency": request_data["source_currency"],
                "quantity": float(trade["quantity"])
            })
            payments.append({
                "sender": trade["party1"][0],
                "recipient": request_data["recipient_wallet_id"],
                "currency": request_data["target_currency"],
                "quantity": round(float(trade["quantity"]) * float(trade["price"]), 2)
            })
    return payments, remaining_order


if __name__ == "__main__":
    app.run(debug=True)
