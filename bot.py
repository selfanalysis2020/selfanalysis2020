import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import time
from threading import Thread

# Telegram Bot details
BOT_TOKEN = "7620868135:AAHSj0VlRj_u_wYiSC9vV0WKBkJnFQ7RR2g"
CHAT_ID = "1797465946"

# Define the URL and the scanning condition
url = "https://chartink.com/screener/process"
condition = {
    "scan_clause": """
    (({cash} (
        latest ema ( latest close , 10 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 20 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 40 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 60 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 80 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 100 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 120 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 140 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 160 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 180 ) < latest ema ( latest close , 200 ) and
        latest ema ( latest close , 10 ) > latest ema ( latest close , 40 ) and
        latest ema ( latest close , 10 ) < latest ema ( latest close , 200 ) * 1.02 and
        market cap > 1000 and
        latest close > 1 day ago high
    )))
    """
}

# Global flag to control the running state
is_running = False
start_time = None  # Track the start time of the bot


def fetch_data():
    """
    Fetches stock data from Chartink and formats it into a table with horizontal separators for Telegram.
    """
    try:
        with requests.Session() as s:
            # Get CSRF token
            r_data = s.get(url)
            soup = bs(r_data.content, "lxml")
            meta = soup.find("meta", {"name": "csrf-token"})["content"]

            # Prepare headers and send POST request
            headers = {"x-csrf-token": meta}
            response = s.post(url, headers=headers, data=condition).json()

            if "data" in response:
                stock_list = pd.DataFrame(response["data"])
                filtered_stock_list = stock_list[["nsecode", "close", "per_chg", "volume"]]

                # Format stock data
                filtered_stock_list["nsecode"] = filtered_stock_list["nsecode"].str.slice(0, 8)
                filtered_stock_list["volume"] = filtered_stock_list["volume"] / 100000
                filtered_stock_list["per_chg"] = filtered_stock_list["per_chg"].apply(lambda x: f"{x:.1f}%")
                filtered_stock_list["volume"] = filtered_stock_list["volume"].apply(lambda x: f"{x:.1f}V")

                filtered_stock_list.sort_values(by="per_chg", ascending=False, inplace=True)

                # Create table header
                header_row = f"| {'SYMBOL'.ljust(5)} | {'Close'.ljust(10)} | {'%Chg'.ljust(6)} | {'Vol(L)'.ljust(6)} |"

                # Calculate the separator length based on the total width of the table
                separator_length = len(header_row)
                separator = '-' * separator_length

                # Add rows with horizontal separators
                rows = []
                for _, row in filtered_stock_list.iterrows():
                    formatted_row = f"| {row['nsecode'].ljust(5)} | {float(row['close']):>10,.1f} | {row['per_chg']:>6} | {row['volume']:>6} |"
                    rows.append(formatted_row)
                    rows.append(separator)  # Add separator after each row

                # Combine all rows into a final table
                table = f"{header_row}\n{separator}\n" + "\n".join(rows)

                return f"Stocks:\n{table}"
            else:
                return "No data found in the response."
    except Exception as e:
        return f"Error while fetching data: {str(e)}"


def send_to_telegram(message):
    """
    Sends a message to the Telegram bot.
    """
    try:
        telegram_url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": message}
        requests.post(telegram_url, data=payload)
    except Exception as e:
        print(f"Error sending to Telegram: {str(e)}")


def run_task():
    """
    Runs the fetching task at regular intervals.
    """
    global is_running, start_time
    start_time = time.time()
    while is_running:
        if time.time() - start_time > 120:  # Check if 2 minutes have passed
            is_running = False
            send_to_telegram("Stopped Fetching Restart.")
            break
        data = fetch_data()
        send_to_telegram(data)
        time.sleep(60)  # Wait for 60 seconds


def listen_for_commands():
    """
    Listens for Telegram commands to start or stop the bot.
    """
    global is_running
    telegram_url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    offset = None

    while True:
        try:
            params = {"offset": offset, "timeout": 10}
            response = requests.get(telegram_url, params=params).json()

            if "result" in response:
                for update in response["result"]:
                    offset = update["update_id"] + 1
                    if "message" in update and "text" in update["message"]:
                        chat_id = update["message"]["chat"]["id"]
                        text = update["message"]["text"].strip().lower()

                        if chat_id == int(CHAT_ID):
                            if text == "/start":
                                if not is_running:
                                    is_running = True
                                    Thread(target=run_task).start()
                                    send_to_telegram("Fetching started. Updates will be sent every 60 seconds.")
                                else:
                                    send_to_telegram("Fetching is already running.")
                            elif text == "/stop":
                                if is_running:
                                    is_running = False
                                    send_to_telegram("Fetching stopped.")
                                else:
                                    send_to_telegram("Fetching is not running.")
                            else:
                                send_to_telegram("Invalid command. Use /start or /stop.")
        except Exception as e:
            print(f"Error while listening for commands: {str(e)}")
            time.sleep(5)
            print("Reconnecting...")


if __name__ == "__main__":
    print("Bot is running. Use /start to begin fetching and /stop to halt.")
    while True:
        try:
            listen_for_commands()
        except Exception as e:
            print(f"Critical error: {str(e)}")
            time.sleep(10)
            print("Restarting bot...")
